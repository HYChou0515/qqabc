coverage: test-func
	. .venv/bin/activate && \
	coverage html
	
test: test-style test-func 
test-func:
	. .venv/bin/activate && \
	rm -rf htmlcov .coverage && \
	coverage run -m pytest tests/tdd && \
	coverage report -m
test-style:
	. .venv/bin/activate && \
	mypy src/qqabc src/qqabc_cli tests/tdd && \
	ruff check src/qqabc src/qqabc_cli tests/tdd
style:
	. .venv/bin/activate && \
	ruff format src/qqabc src/qqabc_cli tests/tdd && \
	ruff check --fix src/qqabc src/qqabc_cli tests/tdd
