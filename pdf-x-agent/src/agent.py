import schedule
import time
import random
import json
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from pathlib import Path
import logging

from src.pdf_processor import PDFProcessor
from src.text_analyzer import TextAnalyzer
from src.vector_store import DocumentVectorStore
from src.x_poster import XPoster
from config.settings import PDF_DIR
from utils.file_handler import FileHandler
from utils.logger import logger

class PDFXAgent:
    def __init__(self):
        self.pdf_processor = PDFProcessor()
        self.analyzer = TextAnalyzer()
        self.vector_store = DocumentVectorStore()
        self.x_poster = XPoster()
        self.file_handler = FileHandler()
        
        # State tracking
        self.state_file = Path(__file__).parent.parent / "data" / "agent_state.json"
        self.load_state()
        
    def load_state(self):
        """Load agent state from file"""
        try:
            if self.state_file.exists():
                self.state = self.file_handler.read_json(self.state_file)
                logger.info(f"Loaded state from {self.state_file}")
            else:
                self.state = {
                    "last_run": None,
                    "processed_files": [],
                    "posted_tweets": [],
                    "total_runs": 0,
                    "errors": []
                }
        except Exception as e:
            logger.error(f"Error loading state: {e}")
            self.state = {
                "last_run": None,
                "processed_files": [],
                "posted_tweets": [],
                "total_runs": 0,
                "errors": []
            }
    
    def save_state(self):
        """Save agent state to file"""
        try:
            self.state["last_run"] = str(datetime.now())
            self.file_handler.write_json(self.state, self.state_file)
            logger.debug("State saved")
        except Exception as e:
            logger.error(f"Error saving state: {e}")
    
    def process_new_documents(self) -> List[Dict[str, Any]]:
        """Find and process new PDF documents"""
        logger.info("🔍 Scanning for new PDF documents...")
        
        # Get all PDF files
        pdf_files = list(PDF_DIR.glob("*.pdf"))
        
        if not pdf_files:
            logger.warning(f"No PDF files found in {PDF_DIR}")
            return []
        
        logger.info(f"Found {len(pdf_files)} PDF files")
        
        # Process new files
        new_documents = []
        for pdf_file in pdf_files:
            file_hash = self.pdf_processor.get_file_hash(pdf_file)
            
            # Check if already processed
            if any(f["filename"] == pdf_file.name and f["hash"] == file_hash 
                  for f in self.state["processed_files"]):
                continue
            
            # Process the PDF
            doc_data = self.pdf_processor.process_single_pdf(pdf_file)
            if doc_data:
                new_documents.append(doc_data)
                
                # Update state
                self.state["processed_files"].append({
                    "filename": pdf_file.name,
                    "hash": file_hash,
                    "processed_at": str(datetime.now())
                })
        
        if new_documents:
            logger.info(f"📄 Processed {len(new_documents)} new documents")
            # Add to vector store
            self.vector_store.add_documents(new_documents)
        
        return new_documents
    
    def select_document_for_posting(self, topic: Optional[str] = None) -> Optional[str]:
        """Select a document to create a post about"""
        try:
            all_docs = self.vector_store.get_all_documents()
            
            if not all_docs:
                logger.warning("No documents in vector store")
                return None
            
            if topic:
                # Search for documents related to topic
                results = self.vector_store.search(topic, n_results=5)
                if results:
                    # Pick a random result
                    selected = random.choice(results)
                    logger.info(f"Selected document about '{topic}': {selected['metadata']['filename']}")
                    return selected['content']
            
            # Pick a random document
            random_doc = random.choice(all_docs)
            chunks = self.vector_store.get_document_chunks(random_doc)
            
            if chunks:
                # Combine first 2-3 chunks
                selected_content = ' '.join([c['content'] for c in chunks[:3]])
                logger.info(f"Randomly selected: {random_doc}")
                return selected_content
            
            return None
            
        except Exception as e:
            logger.error(f"Error selecting document: {e}")
            return None
    
    def create_and_post(self, topic: Optional[str] = None) -> bool:
        """Create a post and publish it"""
        try:
            # Select document content
            content = self.select_document_for_posting(topic)
            
            if not content:
                logger.error("No content available for posting")
                return False
            
            # Analyze content
            logger.info("🧠 Analyzing document content...")
            analysis = self.analyzer.analyze_document(content)
            
            # Generate post
            logger.info("✍️  Generating X post...")
            post = self.analyzer.generate_post(analysis)
            
            # Post to X
            logger.info("🐦 Posting to X...")
            result = self.x_poster.post_tweet(post)
            
            if result and result.get("success"):
                # Log the successful post
                self.state["posted_tweets"].append({
                    "timestamp": str(datetime.now()),
                    "tweet_id": result.get("tweet_id"),
                    "topic": analysis.get("main_topic"),
                    "content_preview": post[:100] + "..."
                })
                self.state["total_runs"] += 1
                self.save_state()
                
                logger.info(f"✅ Successfully posted! Tweet ID: {result.get('tweet_id')}")
                logger.info(f"📝 Content: {post}")
                return True
            else:
                error_msg = result.get("error", "Unknown error") if result else "No result"
                logger.error(f"❌ Post failed: {error_msg}")
                
                # Log error
                self.state["errors"].append({
                    "timestamp": str(datetime.now()),
                    "error": error_msg,
                    "post_preview": post[:100] + "..."
                })
                self.save_state()
                return False
                
        except Exception as e:
            logger.error(f"Error in create_and_post: {e}")
            
            self.state["errors"].append({
                "timestamp": str(datetime.now()),
                "error": str(e)
            })
            self.save_state()
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get agent statistics"""
        vector_stats = self.vector_store.get_stats()
        
        return {
            "agent": {
                "total_runs": self.state["total_runs"],
                "last_run": self.state["last_run"],
                "total_tweets": len(self.state["posted_tweets"]),
                "total_documents": len(self.state["processed_files"]),
                "error_count": len(self.state["errors"])
            },
            "vector_store": vector_stats,
            "models": {
                "llm": self.analyzer.llm_model,
                "embedding": self.analyzer.embedding_model
            }
        }
    
    def interactive_session(self):
        """Interactive mode for manual control"""
        print("\n" + "="*60)
        print("📚 PDF X Agent - Interactive Mode")
        print("="*60)
        
        while True:
            print("\n🔧 Options:")
            print("1. 📄 Process new PDFs")
            print("2. 🧠 Analyze a specific PDF")
            print("3. ✍️  Generate and post (random document)")
            print("4. 🔍 Generate and post (specific topic)")
            print("5. 📊 View statistics")
            print("6. 📋 List all documents")
            print("7. 🚪 Exit")
            
            choice = input("\nSelect option (1-7): ").strip()
            
            if choice == "1":
                print("\n📄 Processing PDFs...")
                new_docs = self.process_new_documents()
                print(f"✓ Processed {len(new_docs)} new documents")
                
            elif choice == "2":
                pdf_name = input("Enter PDF filename (or press Enter to list): ").strip()
                
                if not pdf_name:
                    # List available PDFs
                    pdf_files = list(PDF_DIR.glob("*.pdf"))
                    if pdf_files:
                        print("\nAvailable PDFs:")
                        for i, pdf in enumerate(pdf_files, 1):
                            print(f"  {i}. {pdf.name}")
                        selection = input("\nSelect number or filename: ").strip()
                        
                        if selection.isdigit():
                            idx = int(selection) - 1
                            if 0 <= idx < len(pdf_files):
                                pdf_name = pdf_files[idx].name
                    
                if pdf_name:
                    pdf_path = PDF_DIR / pdf_name
                    if pdf_path.exists():
                        print(f"\nAnalyzing {pdf_name}...")
                        doc_data = self.pdf_processor.process_single_pdf(pdf_path)
                        if doc_data:
                            analysis = self.analyzer.analyze_document(doc_data["content"])
                            print(f"\n📊 Analysis Results:")
                            print(f"  Topic: {analysis.get('main_topic')}")
                            print(f"  Summary: {analysis.get('summary')}")
                            print(f"  Hashtags: {', '.join(analysis.get('hashtags', []))}")
                    else:
                        print(f"❌ PDF not found: {pdf_name}")
                
            elif choice == "3":
                print("\n🎲 Selecting random document...")
                if self.create_and_post():
                    print("✅ Post created and published!")
                else:
                    print("❌ Failed to create post")
                    
            elif choice == "4":
                topic = input("\nEnter topic to search for: ").strip()
                if topic:
                    print(f"\n🔍 Searching for documents about '{topic}'...")
                    if self.create_and_post(topic):
                        print("✅ Post created and published!")
                    else:
                        print("❌ Failed to create post")
                else:
                    print("❌ No topic provided")
                    
            elif choice == "5":
                stats = self.get_stats()
                print("\n📊 Agent Statistics:")
                print(f"  Total runs: {stats['agent']['total_runs']}")
                print(f"  Total tweets: {stats['agent']['total_tweets']}")
                print(f"  Total documents: {stats['agent']['total_documents']}")
                print(f"  Last run: {stats['agent']['last_run']}")
                print(f"  Current model: {stats['models']['llm']}")
                print(f"  Documents in vector store: {stats['vector_store'].get('unique_documents', 0)}")
                
            elif choice == "6":
                docs = self.vector_store.get_all_documents()
                print(f"\n📚 Documents in vector store ({len(docs)}):")
                for i, doc in enumerate(docs, 1):
                    print(f"  {i}. {doc}")
                    
            elif choice == "7":
                print("\n👋 Exiting...")
                break
                
            else:
                print("❌ Invalid option")
    
    def run_scheduled(self, interval_hours: int = 6, topic: Optional[str] = None):
        """Run agent on a schedule"""
        logger.info(f"⏰ Starting scheduled agent (every {interval_hours} hours)")
        
        # Run immediately
        self.run_once(topic)
        
        # Schedule
        schedule.every(interval_hours).hours.do(self.run_once, topic)
        
        print("\n📅 Agent running on schedule. Press Ctrl+C to stop.\n")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
                
                # Show next run time
                next_run = schedule.next_run()
                if next_run:
                    remaining = next_run - datetime.now()
                    print(f"\r⏳ Next run in: {str(remaining).split('.')[0]}", end="", flush=True)
                    
        except KeyboardInterrupt:
            print("\n\n🛑 Agent stopped by user")
    
    def run_once(self, topic: Optional[str] = None):
        """Run the agent once"""
        logger.info("🚀 Starting agent run...")
        
        # Process new documents
        self.process_new_documents()
        
        # Create and post
        success = self.create_and_post(topic)
        
        if success:
            logger.info("✅ Agent run completed successfully")
        else:
            logger.warning("⚠️  Agent run completed with issues")
        
        self.save_state()
        return success