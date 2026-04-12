FROM salehmir/jesse:latest

# Install additional dependencies for our strategies
RUN pip install polars==0.20.31 asyncpg --quiet

# Copy project structure
WORKDIR /jesse-project

# Will be mounted at runtime:
# - /jesse-project/strategies (our strategy files)
# - /data (backtest-results/data)
