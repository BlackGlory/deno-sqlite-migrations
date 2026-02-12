import type { Database } from '@db/sqlite'
import { assert, isFunction } from '@blackglory/prelude'
import { max } from 'extra-utils'

export interface IMigration {
  version: number
  up: string | ((db: Database) => void)
  down: string | ((db: Database) => void)
}
interface IMigrateOptions {
  targetVersion?: number
  throwOnNewerVersion?: boolean
}

export function migrate(
  db: Database
, migrations: IMigration[]
, {
    targetVersion = getMaximumVersion(migrations)
  , throwOnNewerVersion = false
  }: IMigrateOptions = {}
): void {
  const maxVersion = getMaximumVersion(migrations)

  const migrate = db.transaction((
    targetVersion: number
  , maxVersion: number
  ) => {
    const currentVersion = getDatabaseVersion(db)
    if (maxVersion < currentVersion) {
      if (throwOnNewerVersion) {
        throw new Error(`Database version ${currentVersion} is higher than the maximum known migration version.`)
      } else {
        return true
      }
    } else {
      if (currentVersion === targetVersion) {
        return true
      } else if (currentVersion < targetVersion) {
        upgrade()
        return false
      } else {
        downgrade()
        return false
      }
    }
  })

  while (true) {
    const done = migrate.immediate(targetVersion, maxVersion)
    if (done) break
  }

  function upgrade(): void {
    const currentVersion = getDatabaseVersion(db)
    const targetVersion = currentVersion + 1

    const migration = migrations.find(x => x.version === targetVersion)
    assert(migration, `Cannot find a migration for version ${targetVersion}`)

    try {
      if (isFunction(migration.up)) {
        migration.up(db)
      } else {
        db.exec(migration.up)
      }
    } catch (e) {
      throw new Error(
        `Upgrade from version ${currentVersion} to version ${targetVersion} failed.`
      , { cause: e }
      )
    }
    setDatabaseVersion(db, targetVersion)
  }

  function downgrade(): void {
    const currentVersion = getDatabaseVersion(db)
    const targetVersion = currentVersion - 1

    const migration = migrations.find(x => x.version === currentVersion)
    assert(migration, `Cannot find a migration for version ${currentVersion}`)

    try {
      if (isFunction(migration.down)) {
        migration.down(db)
      } else {
        db.exec(migration.down)
      }
    } catch (e) {
      throw new Error(
        `Downgrade from version ${currentVersion} to version ${targetVersion} failed.`
      , { cause: e }
      )
    }
    setDatabaseVersion(db, targetVersion)
  }
}

function getMaximumVersion(migrations: IMigration[]): number {
  return migrations
    .map(x => x.version)
    .reduce(max, 0)
}

function getDatabaseVersion(db: Database): number {
  const result = db.prepare('PRAGMA user_version')
    .get() as { user_version: number }

  return result['user_version']
}

function setDatabaseVersion(db: Database, version: number): void {
  // PRAGMA不支持变量
  db.exec(`PRAGMA user_version = ${version}`)
}
