"""Main orchestrator for all database migrations."""
import sys
import pathlib

# Add scripts/db to path so we can import migrate modules
sys.path.insert(0, '/app/scripts/db')

import migrate_v4
import migrate_v5
import migrate_v5_2
import migrate_v6
import migrate_v7
import verify

from _common import connect, apply_schema


def main():
    """Run all migrations in sequence."""
    print('=' * 70)
    print('STRATEGY BACKTESTING DATABASE MIGRATION')
    print('=' * 70)

    # Step 1: Apply schema
    print('\nStep 1: Applying schema...')
    conn = connect()
    try:
        apply_schema(conn, '/app/scripts/db/schema.sql')
        print('  Schema applied successfully')
    except Exception as e:
        print(f'  ERROR: {e}')
        conn.close()
        return
    finally:
        conn.close()

    # Step 2-6: Run migrations
    migrations = [
        ('v4', migrate_v4),
        ('v5', migrate_v5),
        ('v5_2', migrate_v5_2),
        ('v6', migrate_v6),
        ('v7', migrate_v7)
    ]

    for idx, (name, module) in enumerate(migrations, start=2):
        print(f'\nStep {idx}: Running {name} migration...')
        try:
            module.main()
        except Exception as e:
            print(f'  ERROR during {name} migration: {e}')
            import traceback
            traceback.print_exc()
            return

    # Step 7: Verify
    print(f'\nStep {len(migrations) + 2}: Verifying...')
    try:
        verify.main()
    except Exception as e:
        print(f'  ERROR during verification: {e}')
        import traceback
        traceback.print_exc()
        return

    print('\n' + '=' * 70)
    print('ALL MIGRATIONS COMPLETE')
    print('=' * 70)


if __name__ == '__main__':
    main()
