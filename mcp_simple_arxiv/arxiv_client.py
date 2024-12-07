"""
arXiv API client with rate limiting.
"""

import asyncio
import logging
from datetime import datetime, timedelta
import feedparser
import httpx
from typing import Optional, Dict, List, Any

logger = logging.getLogger(__name__)

class ArxivClient:
    """
    arXiv API client with built-in rate limiting.
    Ensures no more than 1 request every 3 seconds.
    """
    
    def __init__(self):
        self.base_url = "http://export.arxiv.org/api/query"
        self._last_request: Optional[datetime] = None
        self._lock = asyncio.Lock()
        
    async def _wait_for_rate_limit(self) -> None:
        """Ensures we respect arXiv's rate limit of 1 request every 3 seconds."""
        async with self._lock:
            if self._last_request is not None:
                # Calculate time since last request
                elapsed = datetime.now() - self._last_request
                if elapsed < timedelta(seconds=3):
                    # Wait the remaining time
                    await asyncio.sleep(3 - elapsed.total_seconds())
            self._last_request = datetime.now()

    def _clean_text(self, text: str) -> str:
        """Clean up text by removing extra whitespace and newlines."""
        return " ".join(text.split())

    def _parse_entry(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        """Parse a feed entry into a paper dictionary."""
        # Extract PDF and HTML links
        pdf_url = None
        html_url = None
        for link in entry.get('links', []):
            if isinstance(link, dict):
                if link.get('type') == 'application/pdf':
                    pdf_url = link.get('href')
                elif link.get('type') == 'text/html':
                    html_url = link.get('href')

        # Get authors
        authors = []
        for author in entry.get('authors', []):
            if isinstance(author, dict) and 'name' in author:
                authors.append(author['name'])
            elif hasattr(author, 'name'):
                authors.append(author.name)

        # Get categories
        categories = []
        for category in entry.get('tags', []):
            if isinstance(category, dict) and 'term' in category:
                categories.append(category['term'])
            elif hasattr(category, 'term'):
                categories.append(category.term)

        return {
            "id": entry.get('id', '').split("/abs/")[-1].rstrip(),
            "title": self._clean_text(entry.get('title', '')),
            "authors": authors,
            "categories": categories,
            "published": entry.get('published', ''),
            "updated": entry.get('updated', ''),
            "summary": self._clean_text(entry.get('summary', '')),
            "comment": self._clean_text(entry.get('arxiv_comment', '')),
            "journal_ref": entry.get('arxiv_journal_ref', ''),
            "doi": entry.get('arxiv_doi', ''),
            "primary_category": entry.get('arxiv_primary_category', {}).get('term'),
            "pdf_url": pdf_url,
            "html_url": html_url,
        }

    async def search(self, query: str, max_results: int = 10) -> List[Dict[str, Any]]:
        """
        Search arXiv papers.
        
        The query string supports arXiv's advanced search syntax:
        - Search in title: ti:"search terms"
        - Search in abstract: abs:"search terms"
        - Search by author: au:"author name"
        - Combine terms with: AND, OR, ANDNOT
        - Filter by category: cat:cs.AI
        
        Examples:
        - "machine learning"  (searches all fields)
        - ti:"neural networks" AND cat:cs.AI  (title with category)
        - au:bengio AND ti:"deep learning"  (author and title)
        """
        await self._wait_for_rate_limit()
        
        # Ensure max_results is within API limits
        max_results = min(max_results, 2000)  # API limit: 2000 per request
        
        params = {
            "search_query": query,
            "max_results": max_results,
            "sortBy": "submittedDate",  # Default to newest papers first
            "sortOrder": "descending",
        }
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(self.base_url, params=params)
                response.raise_for_status()
                
                # Parse the Atom feed response
                feed = feedparser.parse(response.text)
                
                if not isinstance(feed, dict) or 'entries' not in feed:
                    logger.error("Invalid response from arXiv API")
                    logger.debug(f"Response text: {response.text[:1000]}...")
                    raise ValueError("Invalid response from arXiv API")
                    
                if not feed.get('entries'):
                    # Empty results are ok - return empty list
                    return []
                
                return [self._parse_entry(entry) for entry in feed.entries]
                
            except httpx.HTTPError as e:
                logger.error(f"HTTP error while searching: {e}")
                raise ValueError(f"arXiv API HTTP error: {str(e)}")
            
    async def get_paper(self, paper_id: str) -> Dict[str, Any]:
        """
        Get detailed information about a specific paper.
        
        Args:
            paper_id: arXiv paper ID (e.g., "2103.08220")
            
        Returns:
            Dictionary containing paper metadata
        """
        await self._wait_for_rate_limit()
        
        params = {
            "id_list": paper_id,
            "max_results": 1
        }
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(self.base_url, params=params)
                response.raise_for_status()
                
                feed = feedparser.parse(response.text)
                if not isinstance(feed, dict) or 'entries' not in feed:
                    logger.error("Invalid response from arXiv API")
                    logger.debug(f"Response text: {response.text[:1000]}...")
                    raise ValueError("Invalid response from arXiv API")
                
                if not feed.get('entries'):
                    raise ValueError(f"Paper not found: {paper_id}")
                    
                return self._parse_entry(feed.entries[0])
                
            except httpx.HTTPError as e:
                logger.error(f"HTTP error while fetching paper: {e}")
                raise ValueError(f"arXiv API HTTP error: {str(e)}")
