#!/bin/bash

# generate_coverage.sh - Generate test coverage reports for Zephyros
# Usage: ./generate_coverage.sh

set -e

echo "🧪 Running tests with coverage profiling..."
go test -coverprofile=coverage.out ./...

echo "📊 Generating HTML coverage report..."
go tool cover -html=coverage.out -o coverage.html

echo "📈 Coverage summary:"
go tool cover -func=coverage.out | tail -1

echo ""
echo "✅ Coverage reports generated:"
echo "   📄 coverage.out  - Raw coverage data"
echo "   🌐 coverage.html - Interactive HTML report"
echo "   📋 COVERAGE.md   - Detailed analysis document"
echo ""
echo "🔍 View the HTML report:"
echo "   Open coverage.html in your browser"
echo ""
echo "📊 Quick coverage check:"
echo "   go tool cover -func=coverage.out"
