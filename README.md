# 

TODO: Delete this and the text below, and describe your gem

Welcome to your new gem! In this directory, you'll find the files you need to be able to package up your Ruby library
into a gem. Put your Ruby code in the file `lib/foobara/empty_ruby_project_generator`. To experiment with that code,
run `bin/console` for an interactive prompt.


## Installation

Typical stuff: add `gem "foobara-postgresql-crud-driver"` to your Gemfile or .gemspec file. Or even just
`gem install foobara-postgresql-crud-driver` if just playing with it directly in scripts.

## Usage

TODO: Write usage instructions here

```ruby
#!/usr/bin/env ruby

require "foobara/load_dotenv"
Foobara::LoadDotenv.run!(dir: __dir__)

TODO: some example code
```
## Contributing

Bug reports and pull requests are welcome on GitHub
at https://github.com/foobara/postgresql-crud-driver

## Development

To setup a test database user, on the command line you can:

```
$ sudo -u postgres psql
```

And then:

```sql
CREATE USER testuser WITH PASSWORD 'testpassword' CREATEDB;
```

TODO: add instructions for use with docker

## License

This project is licensed under the MPL-2.0 license. Please see LICENSE.txt for more info.
