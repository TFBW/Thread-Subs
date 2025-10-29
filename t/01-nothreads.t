#!perl
use 5.010;
use strict;
use warnings;
use Test::More;
use Thread::Subs (
    attributes => 1,
    );

my ($a, $name, %pool);

ok(eval <<'', "Declare sub with Thread attribute");
sub test1 :Thread { 1 }
1;

ok($a = Thread::Subs::_attr(\&test1), "Sub has attributes");
is($a->pool, 'DEFAULT', "Default pool");
is($a->clim, 0, "Default clim");
is($a->qlim, 0, "Default qlim");
ok(!$a->void, "Not void by default");
ok($a->shim, "Auto-shim by default");

Thread::Subs->import(attributes => 'noshim');
ok(eval <<'', "Declare sub with multiple attribute values");
sub test2 :Thread(pool=test, clim=1,qlim=10 void) { 2 }
1;

ok($a = Thread::Subs::_attr(\&test2), "Sub has attributes");
is($a->pool, 'test', "Correct pool");
is($a->clim, 1, "Correct clim");
is($a->qlim, 10, "Correct qlim");
ok($a->void, "Is void");
ok(!$a->shim, "Auto-shim disabled");

ok(eval <<'', "Declare sub with pool=SUB");
sub test3 :Thread(pool=SUB) { 3 }
1;

$name = Thread::Subs::_name(\&test3);
ok($a = Thread::Subs::_attr($name), "Sub $name has attributes");
is($a->pool, $name, "Pool matches sub name");

ok(eval <<'', "Declare sub with pool=PKG");
sub test4 :Thread(pool=PKG) { 4 }
1;

$name = Thread::Subs::_name(\&test4);
ok($a = Thread::Subs::_attr($name), "Sub $name has attributes");
is($a->pool, __PACKAGE__, "Pool matches package name");

sub test5 { 5 }
sub test6 { 6 }
sub test7 { 7 }

Thread::Subs::define {
    'main::test5' => { qlim => 5 },
    'main::test6' => { qlim => 6 },
    'main::test7' => { qlim => 7},
};
$a = Thread::Subs::_attr(\&test5);
is($a->qlim, 5, "Hash-based multi-define");

Thread::Subs::define(
    \&test5 => { clim => 5 },
    \&test6 => { clim => 6 },
    \&test7 => { clim => 7 },
    );
$a = Thread::Subs::_attr(\&test6);
is($a->clim, 6, "List-based multi-define");

Thread::Subs::define(\&test7 => clim => 1);
$a = Thread::Subs::_attr(\&test7);
is($a->clim, 1, "List-based single define");

eval <<'';
sub bad_attr :Thread(invalid) { '?' }
1;

ok($@, "Invalid attribute caught");

%pool = Thread::Subs::end_definitions();
is_deeply(
    \%pool,
    { DEFAULT => 6, 'main::test3' => 1, main => 1, test => 1 },
    "Base pool size"
    );

%pool = Thread::Subs::set_pool(DEFAULT => 7, test => 3);
is_deeply(
    \%pool,
    { DEFAULT => 7, 'main::test3' => 1, main => 1, test => 3 },
    "Adjusted pool size"
    );

is(0 + Thread::Subs::end_definitions(), 4, "Number of pools");

eval { Thread::Subs::set_pool(INVALID => 1) };
ok($@, "Invalid pool caught");

eval { Thread::Subs::start_workers() };
ok($@, "Start workers caught");

eval { Thread::Subs::deploy_shims() };
ok($@, "Deploy shims caught");

eval { Thread::Subs::startup() };
ok($@, "Startup caught");

is_deeply([Thread::Subs::running_workers()], [], "No running workers");
is_deeply([Thread::Subs::current_tasks()], [], "No current tasks");

ok(eval { Thread::Subs::stop_workers(); 1 }, "Stop workers");

my $r;
sub new_r { $r = Thread::Subs::result->new }

&new_r;
isa_ok($r, 'Thread::Subs::result');
ok(!$r->ready, "Initially not ready");
ok(!$r->cb, "Initially no callback");
$a = 0;
ok($r->cb(sub { $a = 1 }), "Callback set");
is($a, 0, "Still pending");
$r->send('x');
ok($r->ready, "Ready now");
ok(!$r->failed, "Not failed");
is($a, 1, "Callback executed");
ok(!$r->cb, "Callback cleared");
is($r->recv, 'x', "Data received");
$r->cb(sub { $a = 2 });
is($a, 2, "New callback is immediate");
ok(!$r->cb, "Callback cleared");

&new_r;
$r->cb(sub {
    ok($_[0]->failed, "Failed, in callback");
    is($_[0]->data, 'die', "Failure msg in callback");
       });
$r->croak("die");
eval { $r->recv };
ok($@, "Exception raised");

done_testing();
