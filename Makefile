format:
	poetry run ruff format .

check:
	poetry run ruff check .
	poetry run mypy .

fix:
	poetry run ruff --fix .

format-fix:
	poetry run ruff format . 
	poetry run ruff --fix .