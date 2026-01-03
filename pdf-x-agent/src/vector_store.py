import chromadb
from chromadb.config import Settings
from typing import List, Dict, Any, Optional
import uuid
import logging
from pathlib import Path
from config.settings import EMBEDDINGS_DIR
from src.text_analyzer import TextAnalyzer
from utils.logger import logger

class DocumentVectorStore:
    def __init__(self):
        self.analyzer = TextAnalyzer()
        self.embeddings_dir = EMBEDDINGS_DIR
        
        # Initialize ChromaDB
        self.client = chromadb.PersistentClient(
            path=str(self.embeddings_dir),
            settings=Settings(anonymized_telemetry=False)
        )
        
        # Create or get collection
        try:
            self.collection = self.client.get_or_create_collection(
                name="pdf_documents",
                metadata={"hnsw:space": "cosine", "description": "PDF document embeddings"}
            )
            logger.info("Vector store initialized")
        except Exception as e:
            logger.error(f"Error initializing vector store: {e}")
            raise
    
    def add_document(self, document: Dict[str, Any]) -> bool:
        """Add a single document to vector store"""
        try:
            # Check if document already exists
            existing = self.collection.get(
                where={"filename": document["filename"]},
                limit=1
            )
            
            if existing['ids']:
                logger.info(f"Document {document['filename']} already exists")
                return True
            
            # Chunk the document
            chunks = self.analyzer.chunk_text(document["content"])
            
            # Generate embeddings
            embeddings = self.analyzer.get_embeddings(chunks)
            
            # Prepare metadata for each chunk
            ids = []
            metadatas = []
            documents = []
            
            for i, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
                chunk_id = f"{document['filename']}_{document['hash']}_{i}"
                
                metadata = {
                    "filename": document["filename"],
                    "filepath": document["filepath"],
                    "hash": document["hash"],
                    "chunk_index": i,
                    "total_chunks": len(chunks),
                    **document.get("metadata", {})
                }
                
                ids.append(chunk_id)
                metadatas.append(metadata)
                documents.append(chunk)
            
            # Add to collection
            self.collection.add(
                embeddings=embeddings,
                documents=documents,
                metadatas=metadatas,
                ids=ids
            )
            
            logger.info(f"✓ Added {document['filename']} ({len(chunks)} chunks)")
            return True
            
        except Exception as e:
            logger.error(f"Error adding document {document.get('filename', 'unknown')}: {e}")
            return False
    
    def add_documents(self, documents: List[Dict[str, Any]]) -> int:
        """Add multiple documents to vector store"""
        success_count = 0
        
        for doc in documents:
            if self.add_document(doc):
                success_count += 1
        
        logger.info(f"Added {success_count}/{len(documents)} documents to vector store")
        return success_count
    
    def search(self, query: str, n_results: int = 3, filter_metadata: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """Search for similar documents"""
        try:
            # Generate query embedding
            query_embedding = self.analyzer.get_embeddings([query])[0]
            
            # Search
            if filter_metadata:
                results = self.collection.query(
                    query_embeddings=[query_embedding],
                    n_results=n_results,
                    where=filter_metadata
                )
            else:
                results = self.collection.query(
                    query_embeddings=[query_embedding],
                    n_results=n_results
                )
            
            # Format results
            formatted_results = []
            if results['ids'] and results['ids'][0]:
                for i in range(len(results['ids'][0])):
                    formatted_results.append({
                        'id': results['ids'][0][i],
                        'content': results['documents'][0][i],
                        'metadata': results['metadatas'][0][i],
                        'distance': results['distances'][0][i] if results['distances'] else 0
                    })
            
            logger.info(f"Search found {len(formatted_results)} results")
            return formatted_results
            
        except Exception as e:
            logger.error(f"Error searching vector store: {e}")
            return []
    
    def get_document_chunks(self, filename: str) -> List[Dict[str, Any]]:
        """Get all chunks for a specific document"""
        try:
            results = self.collection.get(
                where={"filename": filename}
            )
            
            chunks = []
            for i in range(len(results['ids'])):
                chunks.append({
                    'id': results['ids'][i],
                    'content': results['documents'][i],
                    'metadata': results['metadatas'][i]
                })
            
            chunks.sort(key=lambda x: x['metadata']['chunk_index'])
            return chunks
            
        except Exception as e:
            logger.error(f"Error getting chunks for {filename}: {e}")
            return []
    
    def get_all_documents(self) -> List[str]:
        """Get list of all document filenames in vector store"""
        try:
            results = self.collection.get()
            filenames = set()
            
            for metadata in results['metadatas']:
                filenames.add(metadata['filename'])
            
            return sorted(list(filenames))
            
        except Exception as e:
            logger.error(f"Error getting document list: {e}")
            return []
    
    def delete_document(self, filename: str) -> bool:
        """Delete a document from vector store"""
        try:
            self.collection.delete(where={"filename": filename})
            logger.info(f"Deleted document: {filename}")
            return True
        except Exception as e:
            logger.error(f"Error deleting document {filename}: {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get vector store statistics"""
        try:
            results = self.collection.get()
            
            total_chunks = len(results['ids']) if results['ids'] else 0
            unique_docs = len(set(m['filename'] for m in results['metadatas'])) if results['metadatas'] else 0
            
            return {
                "total_chunks": total_chunks,
                "unique_documents": unique_docs,
                "collection_name": self.collection.name,
                "embedding_model": self.analyzer.embedding_model
            }
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {}