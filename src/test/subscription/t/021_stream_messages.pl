# Tests that logical decoding messages are emitted and that
# they do not break subscribers
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 3;

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
	"CREATE PUBLICATION pub FOR TABLE tab");

$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION sub CONNECTION '$publisher_connstr' PUBLICATION pub WITH (streaming = on)"
);
$node_cascaded->safe_psql('postgres',
	"CREATE SUBSCRIPTION sub CONNECTION '$subscriber_connstr' PUBLICATION pub WITH (streaming = on)"
);

$node_subscriber->safe_psql('postgres', "ALTER SUBSCRIPTION sub DISABLE");
$node_cascaded->safe_psql('postgres', "ALTER SUBSCRIPTION sub DISABLE");

$node_publisher->safe_psql('postgres', qq(
  insert into tab values (1), (2);
));

# ensure a transactional logical decoding message shows up on the slot

$node_publisher->safe_psql('postgres', qq(
	select pg_logical_emit_message(true, 'a prefix', 'message 1');
));

my $slot_codes_with_message = $node_publisher->safe_psql(
	'postgres', qq(
		select get_byte(data, 0)
		from pg_logical_slot_peek_binary_changes('sub', NULL, NULL,
			'proto_version', '1',
			'publication_names', 'pub',
			'messages', 'true')
    offset 5
));

# 66 77 67 == B M C == BEGIN MESSAGE COMMIT
is($slot_codes_with_message, "66\n77\n67",
	'messages on slot are B M C with message option');

$node_subscriber->safe_psql('postgres', "ALTER SUBSCRIPTION sub ENABLE");
$node_publisher->wait_for_catchup('sub');

my $result =
	$node_subscriber->safe_psql('postgres', "SELECT count(*) FROM tab");
is($result, qq(2), 'rows move to subscriber');

diag $node_subscriber->safe_psql(
	'postgres', qq(
		select *
		from pg_logical_slot_peek_binary_changes('sub', NULL, NULL,
			'proto_version', '1',
			'publication_names', 'pub',
			'messages', 'true')
));

$node_cascaded->safe_psql('postgres', "ALTER SUBSCRIPTION sub ENABLE");
$node_subscriber->wait_for_catchup('sub');

is($node_cascaded->safe_psql('postgres', "SELECT count(*) FROM tab"),
  qq(2), 'rows move to cascaded') ;

$node_cascaded->stop('fast');
$node_subscriber->stop('fast');
$node_publisher->stop('fast');
