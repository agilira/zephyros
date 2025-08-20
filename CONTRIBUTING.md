# Contributing to Zephyros

First, thank you for considering contributing to Zephyros. We appreciate the time and effort you are willing to invest. This document outlines the guidelines for contributing to the project to ensure a smooth and effective process for everyone involved.

## How to Contribute

We welcome contributions in various forms, including:
- Reporting bugs
- Suggesting enhancements
- Improving documentation
- Submitting code changes

### Reporting Bugs

If you encounter a bug, please open an issue on our GitHub repository. A well-documented bug report is crucial for a swift resolution. Please include the following information:

- **Go Version**: The output of `go version`.
- **Operating System**: Your OS and version (e.g., Ubuntu 22.04, macOS 12.6).
- **Clear Description**: A concise but detailed description of the bug.
- **Steps to Reproduce**: A minimal, reproducible example that demonstrates the issue. This could be a small Go program.
- **Expected vs. Actual Behavior**: What you expected to happen and what actually occurred.
- **Logs or Error Messages**: Any relevant logs or error output, formatted as code blocks.
- **Performance Context**: If performance-related, include benchmark results and system specifications.

### Suggesting Enhancements

If you have an idea for a new feature or an improvement to an existing one, please open an issue to start a discussion. This allows us to align on the proposal before any significant development work begins.

For performance enhancements, please include:
- **Benchmark results** showing current performance
- **Expected improvement** with rationale
- **Backward compatibility** considerations

## Development Process

1.  **Fork the Repository**: Start by forking the main Zephyros repository to your own GitHub account.
2.  **Clone Your Fork**: Clone your forked repository to your local machine.
    ```bash
    git clone https://github.com/YOUR_USERNAME/zephyros.git
    cd zephyros
    ```
3.  **Create a Branch**: Create a new branch for your changes. Use a descriptive name (e.g., `fix/race-condition` or `feature/adaptive-batching`).
    ```bash
    git checkout -b your-branch-name
    ```
4.  **Make Changes**: Write your code. Ensure your code adheres to Go's best practices and Zephyros performance standards.
5.  **Format Your Code**: Run `gofmt` to ensure your code is correctly formatted.
    ```bash
    gofmt -w .
    ```
6.  **Add Tests**: If you are adding a new feature or fixing a bug, please add corresponding unit or integration tests. All tests must pass.
    ```bash
    go test ./...
    go test -race ./...  # Always test for race conditions
    ```
7.  **Run Benchmarks**: For performance-related changes, run benchmarks to ensure no regressions.
    ```bash
    go test -bench=. -benchmem
    ```
8.  **Commit Your Changes**: Use a clear and descriptive commit message.
    ```bash
    git commit -m "feat: Add adaptive batching support"
    git commit -m "fix: Resolve race condition in writer sequence"
    git commit -m "perf: Optimize cache line padding for ARM64"
    ```
9.  **Push Your Changes**: Push your changes to your forked repository.
    ```bash
    git push origin your-branch-name
    ```
10. **Open a Pull Request**: Open a pull request from your branch to the `main` branch of the official Zephyros repository. Provide a clear title and description for your PR, referencing any related issues.

## Code Guidelines

### Performance Standards

Zephyros is a performance-critical library. All contributions must maintain or improve performance:

- **Zero Allocations**: Steady-state operations should not allocate memory
- **Lock-Free**: Maintain lock-free design principles
- **Cache-Friendly**: Consider CPU cache implications of data structures
- **Benchmarks Required**: Include benchmarks for new features

### Code Style

- Follow standard Go formatting with `gofmt`
- Use meaningful variable and function names
- Add comments for complex algorithms
- Maintain consistent error handling patterns
- Use atomic operations correctly for lock-free code

### Testing Requirements

- **Unit Tests**: Cover all public APIs
- **Race Tests**: Always run with `-race` flag
- **Benchmark Tests**: Include for performance-critical code
- **Integration Tests**: Test realistic usage scenarios
- **Coverage**: Aim for >90% test coverage

## Performance Considerations

When contributing to Zephyros, keep these performance principles in mind:

### Memory Layout
- Use cache-line padding to prevent false sharing
- Align data structures to cache boundaries
- Minimize memory footprint of hot data structures

### Atomic Operations
- Use appropriate memory ordering semantics
- Minimize atomic operations in hot paths
- Understand acquire/release semantics

### Algorithm Efficiency
- Prefer bitwise operations over arithmetic where possible
- Use powers of 2 for efficient masking
- Minimize branches in hot paths

## Documentation Updates

When adding features or making changes:

1. **Update API Documentation**: Modify relevant files in `docs/`
2. **Update Examples**: Add examples for new features
3. **Update README**: Reflect major changes in main README
4. **Update Benchmarks**: Include new benchmark results

## Review Process

All contributions go through a review process:

1. **Automated Checks**: CI/CD pipeline runs tests, benchmarks, and static analysis
2. **Performance Review**: Benchmarks are checked for regressions
3. **Code Review**: Maintainers review code for correctness and style
4. **Security Review**: Changes are reviewed for security implications

## Getting Help

If you need help with your contribution:

- **Discussions**: Use GitHub Discussions for general questions
- **Issues**: Create an issue for specific problems
- **Documentation**: Check the `docs/` folder for detailed guides

## Recognition

Contributors who make significant improvements to Zephyros will be:
- Listed in the CONTRIBUTORS file
- Mentioned in release notes

## License

By contributing to Zephyros, you agree that your contributions will be licensed under the Mozilla Public License 2.

---

Thank you for helping make Zephyros better.
