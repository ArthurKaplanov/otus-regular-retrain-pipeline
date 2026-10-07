train:
	uv run src/regular_retrain/train.py

.PHONY: create-venv-archive
create-venv-archive:
	@echo "Creating .venv archive..."
	mkdir -p venvs
	chmod +x scripts/create_venv_archive.sh
	./scripts/create_venv_archive.sh
	@echo "Archive created successfully"
