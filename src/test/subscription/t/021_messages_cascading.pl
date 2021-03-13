# Tests that logical decoding messages
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 2;

# Create publisher node
my $node_publisher = get_new_node('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

# Create subscriber node
my $node_subscriber = get_new_node('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->start;

# Create cascaded node
my $node_cascaded = get_new_node('cascaded');
$node_cascaded->init(allows_streaming => 'logical');
$node_cascaded->start;

# Create some preexisting content on publisher
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab_test (a int primary key)");

# Setup structure on subscriber
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab_test (a int primary key)");

# Setup structure on cascaded
$node_cascaded->safe_psql('postgres',
	"CREATE TABLE tab_test (a int primary key)");

# Setup logical replication
my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres', "CREATE PUBLICATION tap_pub FOR TABLE tab_test");

$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr' PUBLICATION tap_pub WITH (streaming = on, messages = on)"
);

my $subscriber_connstr = $node_subscriber->connstr . ' dbname=postgres';
$node_subscriber->safe_psql('postgres', "CREATE PUBLICATION tap_cascaded_pub FOR TABLE tab_test");

$node_cascaded->safe_psql('postgres',
	"CREATE SUBSCRIPTION tap_cascaded_sub CONNECTION '$subscriber_connstr' PUBLICATION tap_cascaded_pub WITH (streaming = on, messages = on)"
);

$node_cascaded->safe_psql('postgres', 'ALTER SUBSCRIPTION tap_cascaded_sub DISABLE');

$node_publisher->safe_psql('postgres', qq(
	BEGIN;
	INSERT INTO tab_test VALUES (1);
	SELECT pg_logical_emit_message(true, 'pgoutput', 'transactional message');
	INSERT INTO tab_test VALUES (2);
	COMMIT;
));

$node_publisher->wait_for_catchup('tap_sub');

my $result = $node_subscriber->safe_psql('postgres', "SELECT a FROM tab_test");

is($result, qq(1
2),
	'data successfully replicates to subscriber');

sleep 1;

$result = $node_subscriber->safe_psql(
	'postgres', qq(
		SELECT encode(substr(data, 1, 1), 'escape')
		FROM pg_logical_slot_peek_binary_changes('tap_cascaded_sub', NULL, NULL,
			'proto_version', '1',
			'publication_names', 'tap_cascaded_pub',
			'messages', 'true')
));

# BEGIN MESSAGE COMMIT
is($result, qq(B
O
R
I
M
I
C),
	'messages show up on the cascaded replication slot');

$node_subscriber->stop('fast');
$node_publisher->stop('fast');
