#!perl
use 5.010;
use strict;
use warnings;
use threads;
use threads::shared;
use Test::More;
use Thread::Subs (
    attributes => 1,
    autostart  => 5,
    );
use Time::HiRes qw(time);

BEGIN {
    plan skip_all => "Test requires Mojo::Promise"
        unless eval "use Mojo::Promise; 1";
}

sub nap { select(undef, undef, undef, $_[0] * 0.01); return @_ }

sub test :Thread { &nap }
sub dies :Thread { nap(5); die "@_\n" }

test(1)->mojo_promise->then(
    sub { is_deeply([@_], [1], "Promise resolved") },
    sub { fail("Promise resolved") },
    )->wait;

dies('DOOMED')->mojo_promise->then(
    sub { fail("Promise rejected") },
    sub { like("@_", qr/DOOMED/, "Promise rejected") },
    )->wait;

my $x = '';
my $p = Mojo::Promise->all(
    map {
        my $n = $_;
        test($n)->mojo_promise->then(sub { $x .= $n })
    } (1,5,3,7)
    )->then(sub { Mojo::IOLoop->stop }, sub { diag(@_) });
$p->ioloop->recurring(0.02 => sub { $x .= '~' });
$p->wait;
is($x, '~1~3~5~7', "Timer and subs run in parallel");

done_testing();
