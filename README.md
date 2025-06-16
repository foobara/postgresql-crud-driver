# Foobara::PostgresqlCrudDriver

Allows persisting Foobara Entity records in a Postgres database

## Installation

Typical stuff: add `gem "foobara-postgresql-crud-driver"` to your Gemfile or .gemspec file. Or even just
`gem install foobara-postgresql-crud-driver` if just playing with it directly in scripts.

## Usage

You can initialize a Foobara::PostgresqlCrudDriver instance with a URL, a credentials hash, or nothing at all
which will use the contents of `ENV["DATABASE_URL"]` by default.

```ruby
crud_driver = Foobara::PostgresqlCrudDriver.new("postgres://testuser:testpassword@localhost/testdb")
Foobara::Persistence.default_crud_driver = crud_driver
```

Note: There is not currently a Foobara migrations feature or system. So you will have to either manually
migrate or leach off of migrations of an existing system such as Rails by using the `foobara-rails-command-connector`
gem. If you think it would be fun to design/implement a migrations system for Foobara, please get in touch!!

## Contributing

Bug reports and pull requests are welcome on GitHub
at https://github.com/foobara/postgresql-crud-driver

## Development

If not using Docker, you can use the system postgres and setup a test database user. On the command line you can:

```
$ sudo -u postgres psql
```

And then:

```sql
CREATE USER testuser WITH PASSWORD 'testpassword' CREATEDB;
```

TODO: add instructions for use with docker and docker-compose.yml file

## License

This project is licensed under the MPL-2.0 license. Please see LICENSE.txt for more info.
