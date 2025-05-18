# QQABC

QQABC is a job management system designed to handle job submissions, status tracking, and result management. It provides a CLI interface for users to interact with the system and supports various storage backends and job types.

## Features

- Submit jobs via file, stdin, or data.
- Consume jobs based on priority.
- Track job statuses and update them with details.
- Upload and download job results.
- Plugin support for custom job types and storage backends.

## Requirements

- Python 3.9 or higher
- Dependencies listed in `pyproject.toml`

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd qqabc
   ```

2. Create a virtual environment and activate it:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### CLI Commands

- **Submit a Job**
  ```bash
  qqabc submit job --file <job-file>
  ```
  Or use stdin:
  ```bash
  cat <job-file> | qqabc submit job
  ```

- **Consume a Job**
  ```bash
  qqabc consume job
  ```

- **Update Job Status**
  ```bash
  qqabc update <job-id> -s <status> -d <detail>
  ```

- **Upload Job Result**
  ```bash
  qqabc upload result --job-id <job-id> --from-file <result-file>
  ```

- **Download Job Result**
  ```bash
  qqabc download result --job-id <job-id> --to-file <output-file>
  ```

### Running Tests

#### Unit Tests
Run unit tests with coverage:
```bash
make test
```

#### BDD Tests
Run behavior-driven tests:
```bash
make test-bdd
```

#### Style Checks
Run static analysis and style checks:
```bash
make test-style
```

## Development

### Directory Structure

- `src/qqabc`: Core application logic.
- `src/qqabc_cli`: CLI interface and commands.
- `tests/tdd`: Unit tests.
- `tests/bdd`: Behavior-driven tests.
- `uml.drawio`: UML diagrams for system design.

### Adding Features

1. Define the feature in a UML diagram (`uml.drawio`).
2. Implement the feature in the appropriate module.
3. Add unit tests in `tests/tdd`.
4. Add BDD scenarios in `tests/bdd/features`.

### Code Style

This project uses `ruff` and `mypy` for linting and type checking. Ensure your code passes these checks before committing.

```bash
make style
```

## License

This project is licensed under the MIT License. See the LICENSE file for details.

## Contact

For questions or contributions, contact [Chou Hung-Yi](mailto:hychou.svm@gmail.com).
