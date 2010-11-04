# gohammer

This is yet another memcached testing tool.  I wrote it because I had
a specific need for a specific traffic pattern and I hadn't written
any code in go lately.

## What it Does

<div>
  <img src="http://public.west.spy.net/images/gohammer.png"
       alt="overview" style="float: right"/>
</div>

Basically, it just opens up a bunch of connections to memcached (based
on the `concurrency` flag) and starts doing whatever the controller
sticks on its channel.

My current test takes a fixed set of keys and does the following
in a loop:

1. Shuffle list of keys and `add` all values.
2. Shuffle list of keys and `get` all values.
3. Shuffle list of keys and `delete` all values.

Every five seconds, the overall rates and number of each type of
command is reported to stdout.

## Usage

    % ./gohammer -h
    flag provided but not defined: -h
    Usage of ./gohammer:
      -prot="tcp": Layer 3 protocol (tcp, tcp4, tcp6)
      -dest="localhost:11211": Host:port to connect to
      -keys=1000000: Number of keys
      -bodylen=20: Number of bytes of value
      -concurrency=32: Number of concurrent clients

...that's about it
