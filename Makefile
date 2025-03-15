coverage: test-func
	. .venv/bin/activate && \
	coverage html
	
test: test-style test-func 
test-func:
	. .venv/bin/activate && \
	rm -rf htmlcov .coverage && \
	coverage run -m pytest src/tests && \
	coverage report -m
test-style:
	. .venv/bin/activate && cd src && \
	mypy qqabc qqabc_cli tests && \
	ruff check qqabc qqabc_cli tests
style:
	. .venv/bin/activate && cd src && \
	ruff format qqabc qqabc_cli tests && \
	ruff check --fix qqabc qqabc_cli tests
