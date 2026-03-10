.PHONY: install lint test run demo clean

install:
	python -m pip install -e .[dev]

lint:
	ruff check .

test:
	pytest -q

run:
	python -m weld_pipeline.cli

demo:
	streamlit run dashboard.py

clean:
	find . -type d -name "__pycache__" -exec rm -r {} +
	rm -rf .pytest_cache
