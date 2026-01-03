#!/usr/bin/env python3
"""
PDF X Agent - Main Entry Point
An AI agent that reads PDFs, analyzes content, and posts on X
"""

import argparse
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.agent import PDFXAgent
from utils.logger import logger

def main():
    """Main function to run the PDF X Agent"""
    
    parser = argparse.ArgumentParser(
        description="PDF X Agent - AI agent that reads PDFs and posts on X",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --mode interactive          # Interactive mode
  %(prog)s --mode once                 # Run once
  %(prog)s --mode once --topic "AI"    # Run once about AI
  %(prog)s --mode scheduled --interval 4  # Run every 4 hours
        """
    )
    
    parser.add_argument('--mode', choices=['once', 'scheduled', 'interactive'], 
                       default='interactive', help='Run mode (default: interactive)')
    parser.add_argument('--interval', type=int, default=6,
                       help='Hours between runs (for scheduled mode)')
    parser.add_argument('--topic', type=str, default=None,
                       help='Specific topic for posts')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       default='INFO', help='Logging level')
    
    args = parser.parse_args()
    
    # Set logging level
    import logging
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    logger.info("="*60)
    logger.info("🚀 Starting PDF X Agent")
    logger.info(f"📚 Model: Qwen2.5:4B")
    logger.info(f"🎯 Mode: {args.mode}")
    logger.info("="*60)
    
    try:
        # Initialize agent
        agent = PDFXAgent()
        
        # Run based on mode
        if args.mode == 'once':
            logger.info("Running once...")
            success = agent.run_once(args.topic)
            sys.exit(0 if success else 1)
            
        elif args.mode == 'scheduled':
            logger.info(f"Running scheduled (every {args.interval} hours)...")
            agent.run_scheduled(args.interval, args.topic)
            
        elif args.mode == 'interactive':
            logger.info("Starting interactive mode...")
            agent.interactive_session()
            
    except KeyboardInterrupt:
        logger.info("\n\n👋 Agent stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()