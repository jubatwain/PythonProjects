import pdfplumber
import PyPDF2
from pathlib import Path
import hashlib
import json
from typing import List, Dict, Any
import logging
from config.settings import PDF_DIR, PROCESSED_DIR
from utils.file_handler import FileHandler
from utils.logger import logger

class PDFProcessor:
    def __init__(self):
        self.pdf_dir = PDF_DIR
        self.processed_dir = PROCESSED_DIR
        self.file_handler = FileHandler()
        
    def extract_text_from_pdf(self, pdf_path: Path) -> str:
        """Extract text from PDF using multiple methods"""
        text = ""
        
        try:
            # Method 1: pdfplumber (better for tables)
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
            
            # Method 2: PyPDF2 (fallback)
            if len(text.strip()) < 100:
                with open(pdf_path, 'rb') as file:
                    pdf_reader = PyPDF2.PdfReader(file)
                    for page in pdf_reader.pages:
                        page_text = page.extract_text()
                        if page_text:
                            text += page_text + "\n"
                            
        except Exception as e:
            logger.error(f"Error extracting text from {pdf_path}: {e}")
            
        return text.strip()
    
    def get_file_hash(self, pdf_path: Path) -> str:
        """Generate MD5 hash for file"""
        with open(pdf_path, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()
    
    def process_single_pdf(self, pdf_path: Path) -> Dict[str, Any]:
        """Process a single PDF file"""
        try:
            # Check if PDF exists
            if not pdf_path.exists():
                logger.error(f"PDF not found: {pdf_path}")
                return None
            
            # Get file hash
            file_hash = self.get_file_hash(pdf_path)
            
            # Check if already processed
            processed_file = self.processed_dir / f"{pdf_path.stem}_{file_hash}.json"
            if processed_file.exists():
                logger.info(f"Already processed: {pdf_path.name}")
                return self.file_handler.read_json(processed_file)
            
            # Extract text
            logger.info(f"Processing: {pdf_path.name}")
            text = self.extract_text_from_pdf(pdf_path)
            
            if not text or len(text) < 100:
                logger.warning(f"Minimal text extracted from {pdf_path.name}")
                return None
            
            # Create document data
            doc_data = {
                "filename": pdf_path.name,
                "filepath": str(pdf_path),
                "hash": file_hash,
                "content": text,
                "metadata": {
                    "pages": text.count('\n') // 50 + 1,
                    "characters": len(text),
                    "words": len(text.split()),
                    "processed_at": str(Path.timestamp(pdf_path))
                }
            }
            
            # Save processed document
            self.file_handler.write_json(doc_data, processed_file)
            logger.info(f"✓ Processed {pdf_path.name} ({len(text)} chars)")
            
            return doc_data
            
        except Exception as e:
            logger.error(f"Error processing {pdf_path}: {e}")
            return None
    
    def process_all_pdfs(self) -> List[Dict[str, Any]]:
        """Process all PDFs in the directory"""
        processed_docs = []
        
        # Get all PDF files
        pdf_files = list(self.pdf_dir.glob("*.pdf"))
        
        if not pdf_files:
            logger.warning(f"No PDF files found in {self.pdf_dir}")
            return processed_docs
        
        logger.info(f"Found {len(pdf_files)} PDF files")
        
        # Process each PDF
        for pdf_file in pdf_files:
            doc_data = self.process_single_pdf(pdf_file)
            if doc_data:
                processed_docs.append(doc_data)
        
        logger.info(f"Total processed: {len(processed_docs)} documents")
        return processed_docs
    
    def chunk_text(self, text: str, chunk_size: int = 1000, overlap: int = 200) -> List[str]:
        """Split text into overlapping chunks"""
        words = text.split()
        chunks = []
        
        if len(words) <= chunk_size:
            return [' '.join(words)]
        
        for i in range(0, len(words), chunk_size - overlap):
            chunk = ' '.join(words[i:i + chunk_size])
            chunks.append(chunk)
            
            if i + chunk_size >= len(words):
                break
        
        logger.debug(f"Split text into {len(chunks)} chunks")
        return chunks