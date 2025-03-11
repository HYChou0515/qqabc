test: test-func test-style
test-func:
	. .venv/bin/activate && cd src && \
	pytest tests
test-style:
	. .venv/bin/activate && cd src && \
	mypy qqabc tests && \
	ruff check qqabc tests
style:
	. .venv/bin/activate && cd src && \
	ruff format qqabc tests && \
	ruff check --fix qqabc tests
