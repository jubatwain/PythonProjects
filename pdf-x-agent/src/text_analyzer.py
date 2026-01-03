import ollama
import json
import re
from typing import List, Dict, Any, Optional
import logging
from config.settings import OLLAMA_BASE_URL, LLM_MODEL, EMBEDDING_MODEL
from utils.logger import logger

class TextAnalyzer:
    def __init__(self):
        self.llm_model = LLM_MODEL  # qwen2.5:4b
        self.embedding_model = EMBEDDING_MODEL  # nomic-embed-text
        
        # Test Ollama connection
        self.test_connection()
    
    def test_connection(self) -> bool:
        """Test if Ollama is running and models are available"""
        try:
            # Check if Ollama is running
            models = ollama.list()
            model_names = [model['name'] for model in models['models']]
            
            logger.info(f"Connected to Ollama. Available models: {', '.join(model_names)}")
            
            # Check if our models are available
            if self.llm_model not in model_names:
                logger.warning(f"LLM model '{self.llm_model}' not found in Ollama.")
                logger.info(f"Available LLM models: {[m for m in model_names if 'embed' not in m]}")
            
            if self.embedding_model not in model_names:
                logger.warning(f"Embedding model '{self.embedding_model}' not found in Ollama.")
                logger.info(f"Available embedding models: {[m for m in model_names if 'embed' in m]}")
            
            return True
            
        except Exception as e:
            logger.error(f"Ollama connection failed: {e}")
            logger.info("Make sure Ollama is running: 'ollama serve'")
            return False
    
    def get_embeddings(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings using Ollama"""
        embeddings = []
        
        for text in texts:
            try:
                response = ollama.embeddings(
                    model=self.embedding_model,
                    prompt=text[:10000]  # Limit text length
                )
                embeddings.append(response["embedding"])
                logger.debug(f"Generated embedding for text ({len(text)} chars)")
            except Exception as e:
                logger.error(f"Error generating embedding: {e}")
                # Return zero vector as fallback
                embeddings.append([0.0] * 768)
        
        return embeddings
    
    def analyze_document(self, text: str, max_chars: int = 3000) -> Dict[str, Any]:
        """Analyze document content using Qwen model"""
        # Limit text for analysis
        text_sample = text[:max_chars]
        
        prompt = f"""Analyze this document and provide information in JSON format:

DOCUMENT TEXT:
{text_sample}

Provide JSON with these keys:
- "main_topic": Main topic/subject (1-5 words)
- "key_points": List of 3-5 key points from the document
- "tone": Writing style (formal, informal, technical, persuasive, etc.)
- "summary": Brief 2-3 sentence summary
- "hashtags": List of 3-5 relevant hashtags starting with #
- "audience": Who this document is for (beginners, experts, general, etc.)

IMPORTANT: Return ONLY valid JSON, no other text."""

        try:
            logger.info(f"Analyzing document with {self.llm_model}...")
            response = ollama.generate(
                model=self.llm_model,
                prompt=prompt,
                options={'temperature': 0.2, 'num_predict': 500}
            )
            
            response_text = response['response'].strip()
            logger.debug(f"Raw response: {response_text[:200]}...")
            
            # Extract JSON from response
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            
            if json_match:
                result = json.loads(json_match.group())
                logger.info(f"✓ Analysis complete. Topic: {result.get('main_topic', 'Unknown')}")
            else:
                logger.warning("Could not parse JSON from response")
                result = {
                    "main_topic": "Document Analysis",
                    "key_points": ["Content analysis completed"],
                    "tone": "informative",
                    "summary": text_sample[:200] + "...",
                    "hashtags": ["#document", "#analysis", "#ai"],
                    "audience": "general"
                }
            
            # Add metadata
            result["analysis_model"] = self.llm_model
            result["text_length"] = len(text)
            result["sample_length"] = len(text_sample)
            
            return result
            
        except Exception as e:
            logger.error(f"Error analyzing document: {e}")
            return {
                "main_topic": "Analysis Error",
                "key_points": ["Failed to analyze document"],
                "tone": "neutral",
                "summary": "Error in document analysis",
                "hashtags": ["#error", "#document"],
                "audience": "general",
                "error": str(e)
            }
    
    def generate_post(self, analysis: Dict[str, Any], max_length: int = 280) -> str:
        """Generate X post using Qwen model"""
        prompt = f"""Create an engaging X (Twitter) post based on this document analysis:

DOCUMENT ANALYSIS:
- Topic: {analysis.get('main_topic', 'Interesting document')}
- Key Points: {', '.join(analysis.get('key_points', ['Insightful content']))[:200]}
- Summary: {analysis.get('summary', 'Valuable information')}
- Tone: {analysis.get('tone', 'informative')}
- Audience: {analysis.get('audience', 'general public')}

Hashtags to consider: {', '.join(analysis.get('hashtags', ['#reading', '#insights']))}

GUIDELINES:
1. Keep post under {max_length} characters
2. Start with something engaging
3. Include 2-3 key insights
4. Use 2-3 relevant hashtags from the list
5. Add 1-2 emojis if appropriate
6. End with a question or call to action

Return ONLY the post text."""

        try:
            logger.info("Generating X post...")
            response = ollama.generate(
                model=self.llm_model,
                prompt=prompt,
                options={'temperature': 0.8, 'num_predict': 150}
            )
            
            post = response['response'].strip()
            
            # Clean up the post
            post = post.replace('"', '')  # Remove quotes
            post = ' '.join(post.split())  # Remove extra whitespace
            
            # Ensure character limit
            if len(post) > max_length:
                post = post[:max_length-3] + "..."
            
            logger.info(f"✓ Post generated ({len(post)} chars)")
            return post
            
        except Exception as e:
            logger.error(f"Error generating post: {e}")
            # Fallback post
            fallback = f"📚 Reading about: {analysis.get('main_topic', 'interesting topics')}\n\nKey insight: {analysis.get('summary', '')[:150]}\n\n{', '.join(analysis.get('hashtags', ['#reading', '#ai']))}"
            return fallback[:max_length]
    
    def summarize_text(self, text: str, max_sentences: int = 3) -> str:
        """Generate a summary of text"""
        prompt = f"Summarize this text in {max_sentences} sentences or less:\n\n{text[:2000]}"
        
        try:
            response = ollama.generate(
                model=self.llm_model,
                prompt=prompt,
                options={'temperature': 0.3}
            )
            return response['response'].strip()
        except Exception as e:
            logger.error(f"Error summarizing text: {e}")
            return text[:500] + "..."