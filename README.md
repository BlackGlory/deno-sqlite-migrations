# deno-sqlite-migrations
A utility for database migrations with [sqlite3].

The module using [user_version] to record the schema version.

[sqlite3]: https://github.com/denodrivers/sqlite3
[user_version]: https://www.sqlite.org/pragma.html#pragma_user_version

## API
```ts
interface IMigration {
  version: number
  up: string | ((db: Database) => void)
  down: string | ((db: Database) => void)
}
```

You may need [migration-files].

[migration-files]: https://github.com/BlackGlory/migration-files

### migrate
```ts
function migrate(
  db: Database
, migrations: IMigration[]
, options?: {
    targetVersion?: number
    throwOnNewerVersion?: boolean = false
  }
): void
```

If `options.targetVersion` is `undefined`,
the maximum version of the `migrations` is used.

When the maximum known migration version is less than the `user_version`,
it means the current instance is outdated.
- When `options.throwOnNewerVersion` is `false` (default),
  it will skip the migration,
  so your outdated instance continues to run.
- When `options.throwOnNewerVersion` is `true`,
  it will throw an error,
  so your outdated instance fails immediately.

#### Can multiple instances migrate in parallel?
Yes, the `user_version` update is visible to every database connection.

Each migration uses `BEGIN IMMEDIATE` to ensure that parallel write transactions fail early.
Therefore, you may need a proper retry strategy.
