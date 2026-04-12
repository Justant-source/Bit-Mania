# Jesse Project Configuration for CryptoEngine Backtesting
#
# Jesse backtesting framework configuration.
# Database is separate from main CryptoEngine DB to avoid conflicts.

config = {
    'env': {
        'databases': {
            'postgres': {
                'driver': 'postgres',
                'host': 'localhost',
                'port': 5432,
                'name': 'jesse_db',
                'username': 'cryptoengine',
                'password': 'CryptoEngine2026!',
            }
        },
        'caching': {
            'driver': 'pickle'
        }
    }
}
