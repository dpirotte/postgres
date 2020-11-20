# Tests that logical decoding messages are emitted and that
# they do not break subscribers
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 2;

my $node_publisher = get_new_node('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

my $node_subscriber = get_new_node('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->start;

my $node_cascaded = get_new_node('cascaded');
$node_cascaded->init(allows_streaming => 'logical');
$node_cascaded->start;

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
my $subscriber_connstr = $node_subscriber->connstr . ' dbname=postgres';

$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab (a int PRIMARY KEY)");
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab (a int PRIMARY KEY)");
$node_cascaded->safe_psql('postgres',
	"CREATE TABLE tab (a int PRIMARY KEY)");

$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION pub FOR TABLE tab");
$node_subscriber->safe_psql('postgres',
	"CREATE PUBLICATION pub2 FOR TABLE tab");

$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION sub CONNECTION '$publisher_connstr' PUBLICATION pub WITH (messages = true)"
);
$node_cascaded->safe_psql('postgres',
	"CREATE SUBSCRIPTION sub2 CONNECTION '$subscriber_connstr' PUBLICATION pub2 WITH (messages = true)"
);

$node_subscriber->safe_psql('postgres', "ALTER SUBSCRIPTION sub DISABLE");
$node_cascaded->safe_psql('postgres', "ALTER SUBSCRIPTION sub2 DISABLE");

# ensure a transactional logical decoding message shows up on the slot

$node_publisher->safe_psql('postgres', qq(
  begin;
	select pg_logical_emit_message(true, 'a prefix', 'message 1');
  insert into tab values (1), (2);
  commit;
));

diag $node_publisher->safe_psql('postgres', qq(
	select * 
		from pg_logical_slot_peek_binary_changes('sub', NULL, NULL,
			'proto_version', '1',
			'publication_names', 'pub',
			'messages', 'true')
));

$node_subscriber->safe_psql('postgres', "ALTER SUBSCRIPTION sub ENABLE");
sleep 2;
# $node_publisher->wait_for_catchup('sub');

my $result =
	$node_subscriber->safe_psql('postgres', "SELECT count(*) FROM tab");
is($result, qq(2), 'rows move to subscriber');
sleep 1;
diag $node_subscriber->safe_psql(
	'postgres', qq(
		select *
		from pg_logical_slot_peek_binary_changes('sub2', NULL, NULL,
			'proto_version', '1',
			'publication_names', 'pub',
			'messages', 'true')
));

$node_cascaded->safe_psql('postgres', "ALTER SUBSCRIPTION sub2 ENABLE");
$node_subscriber->wait_for_catchup('sub2');

is($node_cascaded->safe_psql('postgres', "SELECT count(*) FROM tab"),
  qq(2), 'rows move to cascaded') ;

$node_cascaded->stop('fast');
$node_subscriber->stop('fast');
$node_publisher->stop('fast');
