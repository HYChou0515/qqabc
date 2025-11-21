test-style:
	ruff format --check src && \
	ruff check src && \
	mypy src && \
	pyright src

style:
	ruff format src && \
	ruff check --fix src