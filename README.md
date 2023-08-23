Dupletti
========

Dupletti helps you find duplicate video files. It does this via content based hashing, meaning
it will identify duplicate files via content, even if they have different resolutions, video codecs
or if one video is just a subset of those.

Dupletti comes with a built-in web interface to browse the search results that allows you to
remove or rename files.


Simple Usage
------------
Use `cargo run` to compile and run the program. Pass `--help` as an argument to see a list of all available options:

```
USAGE:
    dupletti [FLAGS] [OPTIONS]

FLAGS:
        --allow-preview     Allows web interface to serve files through preview links. Otherwise file links will be
                            local and use file:// , which is not the best UX. However, this opens up a potential
                            security risk, because it allows access random files from your disk through the web
                            interface. It's recommended to only use this if you bind to an internal interface like
                            127.0.0.1
    -c, --clean-unfound     Whether to remove files from the DB that are not found in path
    -h, --help              Prints help information
        --no-web            Use web interface or not
    -r, --reset-database    The pattern to look for
    -V, --version           Prints version information
    -v, --verbose           Verbose mode (-v, -vv, -vvv, etc.)
        --videohash         Enable similarity-search via color histograms

OPTIONS:
    -b, --bind-address <bind-address>            Binding address of the webinterface [default: 127.0.0.1]
        --commit-batchsize <commit-batchsize>    Database commit batch size [default: 1024]
    -p, --path <path>                            The path to the file to read [default: ]
        --port <port>                            Port of the web-interface [default: 5757]
    -t, --threads <threads>
            Number of threads for parallel processing (1 = single-threaded) [default: 4]
```

By default, Dupletti will search whole directories for duplicates, and then open up
a web-interface on Port 5757, so you can look through the results, and remove or rename any
duplicate files.


License
-------
Distributed Parameter Search is copyrighted (c) 2021 by Thomas Unterthiner and licensed under the
[General Public License (GPL) Version 2 or higher](http://www.gnu.org/licenses/gpl-2.0.html>).
See ``LICENSE.md`` for the full details.
