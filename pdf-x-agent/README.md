# 📚 PDF X Agent 🤖

An AI agent that reads PDFs, analyzes content, and posts summaries on X (Twitter) using Ollama and Qwen2.5:4B.

## ✨ Features
- 📄 **PDF Processing**: Extract text from PDFs using multiple libraries
- 🧠 **AI Analysis**: Analyze content using Qwen2.5:4B via Ollama
- 🔍 **Vector Search**: Semantic search with ChromaDB
- 🐦 **X Posting**: Automated posting to X (Twitter)
- ⏰ **Scheduling**: Run on schedule or manually
- 🔐 **Privacy**: All processing happens locally

## 🚀 Quick Start

### 1. Prerequisites
```bash
# Install Ollama and pull models
ollama pull qwen2.5:4b
ollama pull nomic-embed-text