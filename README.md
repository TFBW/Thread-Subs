# Thread::Subs

Execute selected Perl subroutines concurrently in worker threads with
minimal cognitive overhead -- structured concurrency done right.

## Synopsis

```perl
use threads;
use Thread::Subs;

# Mark a subroutine for threaded execution
sub expensive_operation :Thread {
    my ($data) = @_;
    # ... complex processing ...
    return $value;
}

# Start worker threads (uses reasonable defaults)
Thread::Subs::startup();

# Call returns immediately with a result object
my $result = expensive_operation($data);

# Do other work while expensive_operation runs in parallel
do_something_else();

# Block until result is ready
my $value = $result->recv;
```

## Description

Thread::Subs provides a practical way to execute subroutines in
concurrent worker threads while maintaining a simple programming
model.  It aims for very low cognitive overhead: you declare which
subs should run in threads, start the workers, and then call those
subs like normal functions.  The only visible difference is that the
sub immediately returns a lightweight "result" object, very similar to
AnyEvent's condition variables.

It is a higher-level abstraction of parallel execution than most
threading libraries offer, and facilitates multiple common use
patterns like the following through a single API.

- Fire-and-forget serialised single worker (via "clim=1")
- Fixed size resource pool, e.g. for DB connections
- General CPU-bound activity

This module can be used stand-alone, but it also has ready-made
integrations for AnyEvent, Mojolicious (Mojo::Promise), and Future.
With Future::AsyncAwait, you can await a result from a worker thread
inside an async sub.  Mix classic event-driven code with genuine
parallel execution at will.

### Key Features

- **Simple declaration**: Mark subs with `:Thread` attribute or define via function
- **Auto-shimming**: Sub is called as normal, immediately returns a "result" object
- **Concurrency limits**: Specify parallelism constraints per sub as preferred
- **Queue management**: Optional limits on outstanding requests for backpressure
- **Event loop support**: Integrates with event loops but does not require them
- **Async callbacks**: Set callback for when result is ready, no event loop needed
- **Static worker pools**: Bare minimum of worker thread management and overhead
- **Zero dependencies**: All dependencies are in core as of Perl v5.22

### Caveats and Limitations

- Data passed to/from threaded subs must be thread-shareable (see `threads::shared`)
- Filehandles, code refs, and other special types can't be passed directly
- Callbacks execute in signal handler context (keep them simple)
- Workers can't use callbacks (main thread only)
- Significant per-call overhead compared to normal function calls

## Installation

```bash
cpan Thread::Subs
```

Or manually:

```bash
perl Makefile.PL
make
make test
make install
```

## Quick Start

### Basic Usage

```perl
use threads;
use Thread::Subs;

sub compute :Thread {
    my ($n) = @_;
    # CPU-intensive work
    return $n * $n;
}

Thread::Subs::startup(5);  # 5 workers in default pool

my @results = map { compute($_) } 1..10;
print $_->recv, "\n" for @results;
```

### With Callbacks

```perl
sub fetch_url :Thread {
    my ($url) = @_;
    # Network I/O
    return get($url);
}

Thread::Subs::startup();

my $result = fetch_url($url);
$result->cb(sub {
    my ($r) = @_;
    if ($r->failed) { warn "Failed: ", $r->data }
    else { process_data($r->data) }
});
```

### Concurrency Control

```perl
# Only one instance can run at a time
sub write_log :Thread(clim=1) {
    my ($message) = @_;
    # Safe concurrent access to shared resource
}

# Limit queue depth to prevent memory issues
sub batch_process :Thread(qlim=100) {
    my ($item) = @_;
    # Process item
}

# Dedicated pool for database operations
sub db_query :Thread(pool=DB) {
    my ($sql) = @_;
    # Each worker maintains its own DB connection
}
```

### Integration with Event Loops

```perl
# AnyEvent
use AnyEvent;
my $cv = compute($n)->ae_cv;
$cv->cb(sub { say "Result: ", shift->recv });

# Mojolicious
use Mojo::Promise;
compute($n)->mojo_promise->then(
    sub { say "Success: @_" },
    sub { warn "Error: @_" }
)->wait;

# Future (with Future::AsyncAwait)
use Future::AsyncAwait;
async sub process {
    my $result = await compute($n)->future;
    return $result * 2;
}
```

## Common Use Cases

### CPU-Intensive Parallelism

Distribute CPU-bound work across multiple cores:

```perl
sub crunch_numbers :Thread {
    # Expensive calculation
}

Thread::Subs::startup(7);  # Leave one core free

my @jobs = map { crunch_numbers($_) } @data;
my @results = map { $_->recv } @jobs;
```

### Resource Pool Management

Manage limited resources (database connections, API clients):

```perl
package DB::Worker;
use Thread::Subs;

my $dbh;  # Each worker gets its own connection

sub query :Thread(pool=DB) {
    my ($sql, @bind) = @_;
    $dbh //= DBI->connect(...);  # Lazy connection per worker
    return $dbh->selectall_arrayref($sql, {}, @bind);
}

Thread::Subs::startup(DB => 10);  # 10 DB workers
```

### Sequential Operations with Parallelism

Serialize access to shared resources without blocking:

```perl
sub append_file :Thread(clim=1, pool=SUB) {
    state $fh;
    $fh //= IO::File->new(">>log.txt");
    print $fh @_;
}

# Multiple callers won't interleave writes,
# but main thread never blocks
append_file("Entry 1\n");
append_file("Entry 2\n");
```

## Requirements

- Perl 5.12 or later
- Working `threads` implementation
- Core modules: threads::shared, Scalar::Util, Time::HiRes
- CPAN modules: Sub::Util (1.40+) if Perl <5.022

Optional:
- `AnyEvent` for ->ae_cv support
- `Mojo::Promise` for ->mojo_promise support  
- `Future` for ->future support
- `threads::posix` for better signal handling on non-Linux platforms

## Documentation

Full documentation is available via perldoc:

```bash
perldoc Thread::Subs
```

Or view online at [MetaCPAN](https://metacpan.org/pod/Thread::Subs).

## Testing

```bash
make test
```

Tests cover:
- Basic threading functionality
- Concurrency and queue limits
- Callback mechanisms
- Integration with AnyEvent, Mojolicious, and Future (if installed)
- Worker lifecycle management

## Author

Brett Watson <brett.watson@gmail.com>

## License

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

## See Also

- [threads](https://metacpan.org/pod/threads) - Perl interpreter-based threads
- [threads::shared](https://metacpan.org/pod/threads::shared) - Share variables between threads
- [threads::posix](https://metacpan.org/pod/threads::posix) - Enhanced thread signals via pthreads
- [AnyEvent](https://metacpan.org/pod/AnyEvent) - Event loop framework
- [Future](https://metacpan.org/pod/Future) - Async result objects
- [Mojo::Promise](https://metacpan.org/pod/Mojo::Promise) - Promises/A+ implementation

