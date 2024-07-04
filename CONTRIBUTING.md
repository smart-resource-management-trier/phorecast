# Contributing to Solar Power Forecasting Project

Thank you for your interest in contributing to the Solar Power Forecasting Project! Your contributions help make this project better and more robust. Below are the guidelines to help you get started.

## Table of Contents


2. [How to Contribute](#how-to-contribute)
    - [Reporting Bugs](#reporting-bugs)
    - [Suggesting Enhancements](#suggesting-enhancements)
    - [Contributing Code](#contributing-code)
3. [Development Setup](#development-setup)
4. [Pull Request Process](#pull-request-process)
5. [Style Guides](#style-guides)
    - [Python Style Guide](#python-style-guide)
    - [Commit Messages](#commit-messages)
6. [License](#license)


## How to Contribute

### Reporting Bugs

If you find a bug in the project, please open an issue and mark it as bug on GitHub with the 
following details:

- A clear and descriptive title
- A detailed description of the setup, operating system, and environment
- Steps to reproduce the issue
- Expected and actual results
- Any relevant logs or screenshots

### Suggesting Enhancements

If you have an idea to improve the project, please open an issue and mark it as feature request on 
GitHub with the following details:

- A clear and descriptive title
- A detailed description of the enhancement
- Why this enhancement would be useful
- Any relevant examples or references

### Contributing Code

If you want to contribute code, please follow these steps:

1. Fork the repository.
2. Create a new branch (`git checkout -b feature/YourFeatureName`).
3. Make your changes.
4. Commit your changes (`git commit -m 'Add some feature'`).
5. Push to the branch (`git push origin feature/YourFeatureName`).
6. Open a pull request.

Please ensure that your code follows the style guides and that you have tested your changes, provide
documentation for your enhancement to make it easy to understand.

## Development Setup

To set up your development environment, follow these steps:

1. **Clone the Repository**:
    ```sh 
    git clone https://github.com/yourusername/solar-power-forecasting.git
    cd solar-power-forecasting
    ```

2. **Setup the Virtual Environment**:

    ```sh
    python -m venv .venv
    source .venv/bin/activate  # On Windows use `.\.venv\Scripts\activate`
    pip install -r requirements.txt
    ```

3. **Run the Application**:

    ```sh 
   # 1. The project needs a influxdb database to run. You can run it using docker.
   docker run --env-file .env -p 8086:8086 --name influxdb -d influxdb
   # 2. Run the flask application, ensure that .env is registerd in the environment variables
   flask run app   
    ```
## Style Guides

### Python Style Guide

- Follow [PEP 8](https://www.python.org/dev/peps/pep-0008/) guidelines.
- Use the editorconfig file for consistent formatting.
- Use type hints where possible.
- Write docstrings for all functions and classes.

### Commit Messages

- Use the present tense ("Add feature" not "Added feature").
- Use the imperative mood ("Move cursor to..." not "Moves cursor to...").
- Limit the first line to 72 characters or less.
- Reference issues and pull requests liberally.

## License

By contributing, you agree that your contributions will be licensed under the project's [License](LICENSE).
