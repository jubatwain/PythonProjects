"""
NSE KENYA FUNDAMENTAL STOCK SCREENER
====================================
6-Step Systematic Analysis Framework for Nairobi Securities Exchange

Author: Quantitative Analysis System
Market: Kenya (NSE)
Currency: KES (Kenyan Shillings)

Data Sources:
- Primary: myStocks.co.ke (web scraping)
- Secondary: NSE official data
- Tertiary: Yahoo Finance (for select stocks)

CRITICAL ADJUSTMENTS FOR KENYAN MARKET:
- Market Cap threshold: KES 10 Billion
- Typical P/E ranges: 5-18 (emerging market characteristics)
- Focus on NSE All Share Index (NASI) ~60 listed companies
- Data availability: Limited to 5-7 years vs 10 years in developed markets
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import time
import warnings
import json
import re
from pathlib import Path
import pickle
import yfinance as yf
from typing import Dict, List, Optional, Tuple
import logging

warnings.filterwarnings('ignore')

# ============================================
# CONFIGURATION FOR KENYAN MARKET
# ============================================

class KenyaMarketConfig:
    """Configuration parameters calibrated for NSE Kenya"""
    
    # Market size thresholds (in KES)
    MIN_MARKET_CAP_KES = 10_000_000_000  # 10 Billion KES
    LARGE_CAP_THRESHOLD = 50_000_000_000  # 50 Billion KES
    
    # Financial health criteria
    DEBT_TO_EQUITY_MAX = 1.0
    MIN_PROFIT_YEARS = 5  # Reduced from 8 due to data availability
    MIN_POSITIVE_OCF_YEARS = 3  # Out of last 5 years
    MARGIN_DECLINE_THRESHOLD = 0.33
    
    # Growth criteria (adjusted for emerging market)
    MIN_REVENUE_CAGR = 0.08  # 8%
    MIN_PROFIT_CAGR = 0.08
    GROWTH_QUALITY_RATIO = 0.7
    
    # Valuation benchmarks (Kenyan market typical ranges)
    TYPICAL_PE_RANGE = (5, 18)
    TYPICAL_PB_RANGE = (0.5, 3.5)
    
    # Data parameters
    LOOKBACK_YEARS = 5
    
    # Major NSE stocks to start with (Top 20 by market cap)
    INITIAL_TEST_UNIVERSE = [
        'SCOM',   # Safaricom
        'EQTY',   # Equity Group
        'KCB',    # KCB Group
        'COOP',   # Co-operative Bank
        'ABSA',   # Absa Bank Kenya
        'SCBK',   # Standard Chartered Bank Kenya
        'BAT',    # BAT Kenya
        'EABL',   # East African Breweries
        'BAMB',   # Bamburi Cement
        'TOTL',   # Total Energies Kenya
        'KNRE',   # KenGen
        'KPLC',   # Kenya Power
        'ARM',    # ARM Cement
        'CABL',   # Carbacid
        'NBK',    # National Bank of Kenya
        'DTK',    # D.T. Dobie
        'CTUM',   # Centum Investment
        'BRIT',   # Britam
        'NCBA',   # NCBA Group
        'SBIC'    # Stanbic Holdings
    ]
    
    # NSE sectors
    SECTORS = {
        'Banking': ['EQTY', 'KCB', 'COOP', 'ABSA', 'SCBK', 'NBK', 'NCBA', 'SBIC'],
        'Telecoms': ['SCOM'],
        'Manufacturing': ['BAT', 'EABL', 'BAMB', 'CABL', 'ARM'],
        'Energy': ['TOTL', 'KNRE', 'KPLC'],
        'Investment': ['CTUM', 'BRIT'],
        'Automobile': ['DTK']
    }

config = KenyaMarketConfig()

# ============================================
# LOGGING SETUP
# ============================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'nse_screener_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================
# DATA CACHING SYSTEM
# ============================================

class DataCache:
    """Persistent cache to minimize API calls and web scraping"""
    
    def __init__(self, cache_dir='./nse_kenya_cache'):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        logger.info(f"Cache directory: {self.cache_dir}")
    
    def get_cache_path(self, symbol: str, data_type: str = 'fundamentals') -> Path:
        """Generate cache file path"""
        month_str = datetime.now().strftime('%Y%m')
        return self.cache_dir / f"{symbol}_{data_type}_{month_str}.pkl"
    
    def is_cached(self, symbol: str, data_type: str = 'fundamentals') -> bool:
        """Check if fresh data exists (within current month)"""
        cache_file = self.get_cache_path(symbol, data_type)
        return cache_file.exists()
    
    def save(self, symbol: str, data: any, data_type: str = 'fundamentals'):
        """Save fetched data to cache"""
        cache_file = self.get_cache_path(symbol, data_type)
        with open(cache_file, 'wb') as f:
            pickle.dump({
                'timestamp': datetime.now(),
                'symbol': symbol,
                'data': data
            }, f)
        logger.debug(f"Cached {symbol} {data_type}")
    
    def load(self, symbol: str, data_type: str = 'fundamentals'):
        """Load cached data"""
        cache_file = self.get_cache_path(symbol, data_type)
        try:
            with open(cache_file, 'rb') as f:
                cached = pickle.load(f)
                return cached['data']
        except Exception as e:
            logger.warning(f"Cache load failed for {symbol}: {e}")
            return None
    
    def clear_old_cache(self, months_old: int = 2):
        """Remove cache files older than specified months"""
        cutoff_date = datetime.now() - timedelta(days=months_old * 30)
        removed_count = 0
        for cache_file in self.cache_dir.glob('*.pkl'):
            if cache_file.stat().st_mtime < cutoff_date.timestamp():
                cache_file.unlink()
                removed_count += 1
        if removed_count > 0:
            logger.info(f"Removed {removed_count} old cache files")

cache = DataCache()

# ============================================
# RATE LIMITER
# ============================================

class RateLimiter:
    """Prevent overwhelming servers with requests"""
    
    def __init__(self, calls_per_minute: int = 20):
        self.calls_per_minute = calls_per_minute
        self.min_delay = 60.0 / calls_per_minute
        self.last_call = 0
    
    def wait(self):
        """Wait if necessary to respect rate limit"""
        elapsed = time.time() - self.last_call
        if elapsed < self.min_delay:
            sleep_time = self.min_delay - elapsed
            time.sleep(sleep_time)
        self.last_call = time.time()

rate_limiter = RateLimiter(calls_per_minute=20)

# ============================================
# NSE KENYA DATA FETCHING MODULE
# ============================================

class NSEKenyaDataFetcher:
    """
    Fetch data from multiple sources for NSE Kenya stocks
    Priority: myStocks.co.ke > NSE official > Yahoo Finance
    """
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        self.base_url_mystocks = "https://www.mystocks.co.ke"
        self.base_url_nse = "https://www.nse.co.ke"
    
    def fetch_stock_list(self) -> pd.DataFrame:
        """
        Fetch list of all NSE listed companies
        Returns DataFrame with columns: Symbol, Name, Sector, Market_Cap
        """
        logger.info("Fetching NSE stock universe...")
        
        try:
            url = f"{self.base_url_mystocks}/listed-companies"
            response = self.session.get(url, timeout=15)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                stocks_data = []
                
                tables = soup.find_all('table')
                for table in tables:
                    rows = table.find_all('tr')[1:]
                    for row in rows:
                        cols = row.find_all('td')
                        if len(cols) >= 3:
                            symbol = cols[0].text.strip()
                            name = cols[1].text.strip()
                            sector = cols[2].text.strip() if len(cols) > 2 else 'Unknown'
                            
                            stocks_data.append({
                                'Symbol': symbol,
                                'Company_Name': name,
                                'Sector': sector
                            })
                
                if stocks_data:
                    df = pd.DataFrame(stocks_data)
                    logger.info(f"Fetched {len(df)} NSE stocks from myStocks.co.ke")
                    return df
        
        except Exception as e:
            logger.warning(f"Failed to fetch from myStocks.co.ke: {e}")
        
        # Fallback: Use predefined major stocks
        logger.info("Using predefined stock universe (Top 20 NSE stocks)")
        stocks_data = []
        
        for symbol in config.INITIAL_TEST_UNIVERSE:
            sector = 'Unknown'
            for sect, symbols in config.SECTORS.items():
                if symbol in symbols:
                    sector = sect
                    break
            
            stocks_data.append({
                'Symbol': symbol,
                'Company_Name': self._get_company_name(symbol),
                'Sector': sector
            })
        
        return pd.DataFrame(stocks_data)
    
    def _get_company_name(self, symbol: str) -> str:
        """Map symbol to company name"""
        name_mapping = {
            'SCOM': 'Safaricom PLC',
            'EQTY': 'Equity Group Holdings',
            'KCB': 'KCB Group',
            'COOP': 'Co-operative Bank of Kenya',
            'ABSA': 'Absa Bank Kenya',
            'SCBK': 'Standard Chartered Bank Kenya',
            'BAT': 'BAT Kenya',
            'EABL': 'East African Breweries',
            'BAMB': 'Bamburi Cement',
            'TOTL': 'Total Energies Kenya',
            'KNRE': 'KenGen',
            'KPLC': 'Kenya Power & Lighting',
            'ARM': 'ARM Cement',
            'CABL': 'Carbacid Investments',
            'NBK': 'National Bank of Kenya',
            'DTK': 'D.T. Dobie',
            'CTUM': 'Centum Investment',
            'BRIT': 'Britam Holdings',
            'NCBA': 'NCBA Group',
            'SBIC': 'Stanbic Holdings'
        }
        return name_mapping.get(symbol, symbol)
    
    def fetch_company_fundamentals(self, symbol: str) -> Optional[Dict]:
        """
        Fetch comprehensive financial data for a stock
        Returns dict with: financials, balance_sheet, cash_flow, info
        """
        logger.info(f"Fetching fundamentals for {symbol}")
        rate_limiter.wait()
        
        # Try multiple sources
        data = self._fetch_from_mystocks(symbol)
        if data:
            return data
        
        data = self._fetch_from_yahoo(symbol)
        if data:
            return data
        
        logger.warning(f"No data sources available for {symbol}")
        return None
    
    def _fetch_from_mystocks(self, symbol: str) -> Optional[Dict]:
        """Scrape myStocks.co.ke for financial data"""
        try:
            url = f"{self.base_url_mystocks}/stock/{symbol.lower()}"
            response = self.session.get(url, timeout=15)
            
            if response.status_code != 200:
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            financials_data = {}
            balance_sheet_data = {}
            cash_flow_data = {}
            info_data = {}
            
            # Extract company information
            info_data['longName'] = soup.find('h1', class_='stock-name').text.strip() if soup.find('h1', class_='stock-name') else symbol
            info_data['sector'] = soup.find('span', class_='sector').text.strip() if soup.find('span', class_='sector') else 'Unknown'
            
            # Extract current price and market cap
            price_elem = soup.find('span', class_='current-price')
            if price_elem:
                info_data['currentPrice'] = self._parse_number(price_elem.text)
            
            market_cap_elem = soup.find('span', text=re.compile('Market Cap'))
            if market_cap_elem:
                market_cap_text = market_cap_elem.find_next('span').text
                info_data['marketCap'] = self._parse_market_cap(market_cap_text)
            
            # Extract financial tables
            tables = soup.find_all('table', class_='financial-table')
            
            for table in tables:
                table_header = table.find('thead')
                if not table_header:
                    continue
                
                header_text = table_header.text.lower()
                year_cells = table_header.find_all('th')[1:]
                years = [cell.text.strip() for cell in year_cells]
                
                rows = table.find('tbody').find_all('tr')
                
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) < 2:
                        continue
                    
                    metric_name = cells[0].text.strip()
                    values = [self._parse_number(cell.text) for cell in cells[1:]]
                    
                    series_data = pd.Series(values, index=years)
                    
                    if 'income' in header_text or 'profit' in header_text:
                        financials_data[metric_name] = series_data
                    elif 'balance' in header_text or 'assets' in header_text:
                        balance_sheet_data[metric_name] = series_data
                    elif 'cash' in header_text or 'flow' in header_text:
                        cash_flow_data[metric_name] = series_data
            
            # Standardize field names
            financials_df = pd.DataFrame(financials_data).T
            balance_sheet_df = pd.DataFrame(balance_sheet_data).T
            cash_flow_df = pd.DataFrame(cash_flow_data).T
            
            # Ensure chronological order
            if not financials_df.empty:
                financials_df = financials_df.sort_index(axis=1)
            if not balance_sheet_df.empty:
                balance_sheet_df = balance_sheet_df.sort_index(axis=1)
            if not cash_flow_df.empty:
                cash_flow_df = cash_flow_df.sort_index(axis=1)
            
            return {
                'financials': financials_df,
                'balance_sheet': balance_sheet_df,
                'cash_flow': cash_flow_df,
                'info': info_data,
                'source': 'myStocks.co.ke'
            }
        
        except Exception as e:
            logger.debug(f"myStocks.co.ke fetch failed for {symbol}: {e}")
            return None
    
    def _fetch_from_yahoo(self, symbol: str) -> Optional[Dict]:
        """Fetch from Yahoo Finance (limited coverage for NSE Kenya)"""
        try:
            ticker = yf.Ticker(f"{symbol}.NR")
            
            financials = ticker.financials.T if hasattr(ticker, 'financials') else pd.DataFrame()
            balance_sheet = ticker.balance_sheet.T if hasattr(ticker, 'balance_sheet') else pd.DataFrame()
            cash_flow = ticker.cashflow.T if hasattr(ticker, 'cashflow') else pd.DataFrame()
            info = ticker.info if hasattr(ticker, 'info') else {}
            
            if financials.empty and balance_sheet.empty:
                return None
            
            if not financials.empty:
                financials = financials.sort_index()
            if not balance_sheet.empty:
                balance_sheet = balance_sheet.sort_index()
            if not cash_flow.empty:
                cash_flow = cash_flow.sort_index()
            
            return {
                'financials': financials,
                'balance_sheet': balance_sheet,
                'cash_flow': cash_flow,
                'info': info,
                'source': 'Yahoo Finance'
            }
        
        except Exception as e:
            logger.debug(f"Yahoo Finance fetch failed for {symbol}: {e}")
            return None
    
    def _parse_number(self, text: str) -> Optional[float]:
        """Parse number from text (handles KES formatting)"""
        if not text or text in ['-', 'N/A', '']:
            return None
        
        text = text.replace('KES', '').replace('Ksh', '').replace(',', '').strip()
        
        multiplier = 1
        if 'B' in text.upper():
            multiplier = 1_000_000_000
            text = text.upper().replace('B', '')
        elif 'M' in text.upper():
            multiplier = 1_000_000
            text = text.upper().replace('M', '')
        elif 'K' in text.upper():
            multiplier = 1_000
            text = text.upper().replace('K', '')
        
        try:
            return float(text) * multiplier
        except:
            return None
    
    def _parse_market_cap(self, text: str) -> Optional[float]:
        """Parse market cap value in KES"""
        return self._parse_number(text)
    
    def fetch_sector_peers(self, symbol: str, sector: str, top_n: int = 5) -> List[str]:
        """Get peer companies in same sector"""
        if sector in config.SECTORS:
            peers = [s for s in config.SECTORS[sector] if s != symbol]
            return peers[:top_n]
        return []
    
    def fetch_corporate_announcements(self, symbol: str, months: int = 6) -> List[Dict]:
        """
        Fetch recent corporate announcements
        Returns list of dicts with: date, title, content
        """
        try:
            url = f"{self.base_url_mystocks}/announcements/{symbol.lower()}"
            response = self.session.get(url, timeout=15)
            
            if response.status_code != 200:
                return []
            
            soup = BeautifulSoup(response.content, 'html.parser')
            announcements = []
            
            announcement_divs = soup.find_all('div', class_='announcement-item')
            
            for div in announcement_divs[:20]:
                date_elem = div.find('span', class_='announcement-date')
                title_elem = div.find('h3', class_='announcement-title')
                content_elem = div.find('div', class_='announcement-content')
                
                if date_elem and title_elem:
                    announcements.append({
                        'date': date_elem.text.strip(),
                        'title': title_elem.text.strip(),
                        'content': content_elem.text.strip() if content_elem else ''
                    })
            
            return announcements
        
        except Exception as e:
            logger.debug(f"Announcements fetch failed for {symbol}: {e}")
            return []

# Initialize data fetcher
data_fetcher = NSEKenyaDataFetcher()

# ============================================
# STEP 1: BUSINESS UNDERSTANDING
# ============================================

def analyze_business_basics(symbol: str, company_data: Dict) -> Dict:
    """
    Extract and display basic company information
    """
    info = company_data.get('info', {})
    
    result = {
        'Symbol': symbol,
        'Company_Name': info.get('longName', data_fetcher._get_company_name(symbol)),
        'Sector': info.get('sector', 'Unknown'),
        'Industry': info.get('industry', info.get('sector', 'Unknown')),
        'Data_Source': company_data.get('source', 'Unknown'),
        'Business_Summary': info.get('longBusinessSummary', 'N/A')[:200] + "..." if info.get('longBusinessSummary') else 'N/A',
        'Human_Review_Flag': "⚠️ USER REVIEW REQUIRED: Can you explain this company's core business in one simple sentence?"
    }
    
    return result

# ============================================
# STEP 2: INDUSTRY & COMPETITIVE MOAT
# ============================================

def calculate_moat_indicators(company_data: Dict) -> Optional[Dict]:
    """
    Calculate ROE, ROCE, Operating Margin trends over available years
    """
    try:
        financials = company_data.get('financials', pd.DataFrame())
        balance_sheet = company_data.get('balance_sheet', pd.DataFrame())
        
        if financials.empty or balance_sheet.empty:
            logger.warning("Insufficient data for moat calculation")
            return None
        
        # Extract fields with flexible naming
        net_income = None
        for col in ['Net Income', 'Profit After Tax', 'Net Profit', 'PAT']:
            if col in financials.columns:
                net_income = financials[col]
                break
        
        total_equity = None
        for col in ['Total Stockholder Equity', 'Shareholders Equity', 'Total Equity', 'Equity']:
            if col in balance_sheet.columns:
                total_equity = balance_sheet[col]
                break
        
        total_assets = None
        for col in ['Total Assets', 'Assets']:
            if col in balance_sheet.columns:
                total_assets = balance_sheet[col]
                break
        
        total_debt = None
        for col in ['Total Debt', 'Long Term Debt', 'Debt']:
            if col in balance_sheet.columns:
                total_debt = balance_sheet[col]
                break
        
        if total_debt is None:
            total_debt = pd.Series(0, index=balance_sheet.index)
        
        operating_income = None
        for col in ['Operating Income', 'EBIT', 'Operating Profit']:
            if col in financials.columns:
                operating_income = financials[col]
                break
        
        revenue = None
        for col in ['Total Revenue', 'Revenue', 'Sales', 'Turnover']:
            if col in financials.columns:
                revenue = financials[col]
                break
        
        # Calculate ROE
        roe = None
        if net_income is not None and total_equity is not None:
            roe = (net_income / total_equity * 100).dropna()
        
        # Calculate ROCE
        roce = None
        if operating_income is not None and total_assets is not None and total_equity is not None:
            capital_employed = total_assets - (total_assets - total_equity - total_debt)
            roce = (operating_income / capital_employed * 100).dropna()
        
        # Calculate Operating Margin
        operating_margin = None
        if operating_income is not None and revenue is not None:
            operating_margin = (operating_income / revenue * 100).dropna()
        
        # Get averages over available period
        avg_roe = roe.tail(5).mean() if roe is not None and len(roe) >= 3 else None
        avg_roce = roce.tail(5).mean() if roce is not None and len(roce) >= 3 else None
        avg_margin = operating_margin.tail(5).mean() if operating_margin is not None and len(operating_margin) >= 3 else None
        
        # Calculate moat quality score
        moat_score = calculate_moat_score(avg_roe, avg_roce, avg_margin)
        
        return {
            'ROE_5Y_Avg': round(avg_roe, 2) if avg_roe else None,
            'ROCE_5Y_Avg': round(avg_roce, 2) if avg_roce else None,
            'Operating_Margin_5Y_Avg': round(avg_margin, 2) if avg_margin else None,
            'ROE_Trend': roe.to_dict() if roe is not None else {},
            'ROCE_Trend': roce.to_dict() if roce is not None else {},
            'Margin_Trend': operating_margin.to_dict() if operating_margin is not None else {},
            'Moat_Quality_Score': moat_score
        }
    
    except Exception as e:
        logger.error(f"Moat calculation error: {e}")
        return None

def calculate_moat_score(roe: Optional[float], roce: Optional[float], margin: Optional[float]) -> int:
    """
    Scoring system for competitive moat (0-10 scale)
    Adjusted thresholds for Kenyan market
    """
    if None in [roe, roce, margin]:
        return 0
    
    score = 0
    
    # ROE scoring
    if roe > 15:
        score += 3
    elif roe > 12:
        score += 2
    elif roe > 8:
        score += 1
    
    # ROCE scoring
    if roce > 15:
        score += 3
    elif roce > 12:
        score += 2
    elif roce > 8:
        score += 1
    
    # Operating Margin scoring
    if margin > 15:
        score += 4
    elif margin > 10:
        score += 3
    elif margin > 5:
        score += 2
    elif margin > 0:
        score += 1
    
    return min(score, 10)

def analyze_competitive_position(symbol: str, sector: str, company_data: Dict) -> Dict:
    """
    Analyze company's competitive position vs peers
    """
    peers = data_fetcher.fetch_sector_peers(symbol, sector)
    moat_metrics = calculate_moat_indicators(company_data)
    
    info = company_data.get('info', {})
    market_cap = info.get('marketCap', 0)
    
    market_cap_billions = market_cap / 1_000_000_000 if market_cap else 0
    
    result = {
        'Competitors': peers,
        'Market_Cap_KES_B': round(market_cap_billions, 2),
        'Moat_Metrics': moat_metrics,
        'Human_Review_Flag': "⚠️ USER REVIEW REQUIRED: Based on metrics and competitor list, assess the durability of competitive advantage (brand, cost, regulatory)."
    }
    
    return result

# ============================================
# STEP 3: FINANCIAL HEALTH FILTERS
# ============================================

def apply_financial_health_filters(symbol: str, company_data: Dict) -> Tuple[bool, Dict]:
    """
    Sequential elimination filters - CRITICAL STEP
    Returns: (Pass/Fail, dict of results)
    """
    financials = company_data.get('financials', pd.DataFrame())
    balance_sheet = company_data.get('balance_sheet', pd.DataFrame())
    cash_flow = company_data.get('cash_flow', pd.DataFrame())
    
    results = {'Symbol': symbol, 'Filter_Results': {}}
    
    if financials.empty:
        results['Status'] = 'REJECTED'
        results['Reason'] = 'No financial data available'
        return False, results
    
    # Extract Net Income with flexible column naming
    net_income = None
    for col in ['Net Income', 'Profit After Tax', 'Net Profit', 'PAT']:
        if col in financials.columns:
            net_income = financials[col]
            break
    
    if net_income is None:
        results['Status'] = 'REJECTED'
        results['Reason'] = 'Net Income data not found'
        return False, results
    
    # FILTER 1: Profit Consistency
    available_years = len(net_income)
    required_years = min(config.MIN_PROFIT_YEARS, available_years)
    
    last_n_years = net_income.tail(required_years)
    profit_check = (last_n_years > 0).all() if len(last_n_years) == required_years else False
    
    results['Filter_Results']['Profit_Consistency'] = profit_check
    results['Filter_Results']['Years_Checked'] = required_years
    
    if not profit_check:
        results['Status'] = 'REJECTED'
        results['Reason'] = f'Profit not positive for all last {required_years} years'
        return False, results
    
    # FILTER 2: Debt-to-Equity Ratio
    if not balance_sheet.empty:
        total_debt = None
        for col in ['Total Debt', 'Long Term Debt', 'Debt']:
            if col in balance_sheet.columns:
                total_debt = balance_sheet[col].iloc[-1] if not balance_sheet[col].empty else 0
                break
        
        total_equity = None
        for col in ['Total Stockholder Equity', 'Shareholders Equity', 'Total Equity', 'Equity']:
            if col in balance_sheet.columns:
                total_equity = balance_sheet[col].iloc[-1] if not balance_sheet[col].empty else 0
                break
        
        if total_equity is None or total_equity == 0:
            total_assets = None
            total_liabilities = None
            for col in ['Total Assets', 'Assets']:
                if col in balance_sheet.columns:
                    total_assets = balance_sheet[col].iloc[-1] if not balance_sheet[col].empty else 0
                    break
            for col in ['Total Liabilities', 'Liabilities']:
                if col in balance_sheet.columns:
                    total_liabilities = balance_sheet[col].iloc[-1] if not balance_sheet[col].empty else 0
                    break
            
            if total_assets is not None and total_liabilities is not None:
                total_equity = total_assets - total_liabilities
        
        debt_to_equity = 0
        if total_debt is not None and total_equity is not None and total_equity > 0:
            debt_to_equity = total_debt / total_equity
        
        debt_check = debt_to_equity < config.DEBT_TO_EQUITY_MAX
        results['Filter_Results']['Debt_to_Equity'] = round(debt_to_equity, 2)
        results['Filter_Results']['Debt_Check_Pass'] = debt_check
        
        if not debt_check:
            results['Status'] = 'REJECTED'
            results['Reason'] = f'Debt-to-Equity too high: {debt_to_equity:.2f}'
            return False, results
    
    else:
        results['Filter_Results']['Debt_to_Equity'] = 'N/A'
        results['Filter_Results']['Debt_Check_Pass'] = True
    
    # FILTER 3: Cash Flow Quality
    if not cash_flow.empty:
        operating_cf = None
        for col in ['Operating Cash Flow', 'Cash from Operations', 'Operating Activities']:
            if col in cash_flow.columns:
                operating_cf = cash_flow[col]
                break
        
        if operating_cf is not None and net_income is not None:
            common_years = operating_cf.index.intersection(net_income.index)
            if len(common_years) >= config.MIN_POSITIVE_OCF_YEARS:
                ocf_quality = (operating_cf[common_years] > net_income[common_years]).sum()
                cf_check = ocf_quality >= config.MIN_POSITIVE_OCF_YEARS
                
                results['Filter_Results']['OCF_Quality_Years'] = int(ocf_quality)
                results['Filter_Results']['CF_Check_Pass'] = cf_check
                
                if not cf_check:
                    results['Status'] = 'REJECTED'
                    results['Reason'] = f'Poor cash flow quality: Only {ocf_quality} years OCF > Profit'
                    return False, results
            else:
                results['Filter_Results']['OCF_Quality_Years'] = 'Insufficient Data'
                results['Filter_Results']['CF_Check_Pass'] = True
        else:
            results['Filter_Results']['OCF_Quality_Years'] = 'N/A'
            results['Filter_Results']['CF_Check_Pass'] = True
    else:
        results['Filter_Results']['OCF_Quality_Years'] = 'N/A'
        results['Filter_Results']['CF_Check_Pass'] = True
    
    # FILTER 4: Margin Stability
    operating_income = None
    revenue = None
    
    for col in ['Operating Income', 'EBIT', 'Operating Profit']:
        if col in financials.columns:
            operating_income = financials[col]
            break
    
    for col in ['Total Revenue', 'Revenue', 'Sales', 'Turnover']:
        if col in financials.columns:
            revenue = financials[col]
            break
    
    if operating_income is not None and revenue is not None:
        operating_margin = (operating_income / revenue * 100).dropna()
        
        if len(operating_margin) >= 3:
            last_3y_margins = operating_margin.tail(3)
            if len(last_3y_margins) >= 2:
                peak_margin = last_3y_margins.max()
                current_margin = last_3y_margins.iloc[-1]
                
                if peak_margin > 0:
                    margin_decline = (peak_margin - current_margin) / peak_margin
                    margin_check = margin_decline < config.MARGIN_DECLINE_THRESHOLD
                    
                    results['Filter_Results']['Margin_Decline_Pct'] = round(margin_decline * 100, 1)
                    results['Filter_Results']['Margin_Check_Pass'] = margin_check
                    
                    if not margin_check:
                        results['Status'] = 'REJECTED'
                        results['Reason'] = f'Operating margin declined {margin_decline*100:.1f}% from peak'
                        return False, results
                else:
                    results['Filter_Results']['Margin_Decline_Pct'] = 'N/A'
                    results['Filter_Results']['Margin_Check_Pass'] = True
            else:
                results['Filter_Results']['Margin_Decline_Pct'] = 'Insufficient Data'
                results['Filter_Results']['Margin_Check_Pass'] = True
        else:
            results['Filter_Results']['Margin_Decline_Pct'] = 'Insufficient Data'
            results['Filter_Results']['Margin_Check_Pass'] = True
    else:
        results['Filter_Results']['Margin_Decline_Pct'] = 'N/A'
        results['Filter_Results']['Margin_Check_Pass'] = True
    
    # ALL FILTERS PASSED
    results['Status'] = 'PASSED'
    results['Human_Review_Flag'] = "✅ Financials quantitatively sound. Review annual report notes for accounting red flags."
    
    return True, results

# ============================================
# STEP 4: GROWTH POTENTIAL
# ============================================

def calculate_growth_metrics(company_data: Dict) -> Optional[Dict]:
    """
    Calculate revenue/profit CAGR and growth quality
    """
    try:
        financials = company_data.get('financials', pd.DataFrame())
        cash_flow = company_data.get('cash_flow', pd.DataFrame())
        
        if financials.empty:
            return None
        
        # Extract data with flexible naming
        revenue = None
        for col in ['Total Revenue', 'Revenue', 'Sales', 'Turnover']:
            if col in financials.columns:
                revenue = financials[col]
                break
        
        net_profit = None
        for col in ['Net Income', 'Profit After Tax', 'Net Profit', 'PAT']:
            if col in financials.columns:
                net_profit = financials[col]
                break
        
        operating_cf = None
        if not cash_flow.empty:
            for col in ['Operating Cash Flow', 'Cash from Operations', 'Operating Activities']:
                if col in cash_flow.columns:
                    operating_cf = cash_flow[col]
                    break
        
        # Initialize results
        results = {
            'Revenue_CAGR_5Y': None,
            'Profit_CAGR_5Y': None,
            'Growth_Quality_Ratio': None,
            'Recent_Revenue_Growth': None,
            'Recent_Profit_Growth': None,
            'Passes_Growth_Criteria': False,
            'Growth_Data_Years': 0
        }
        
        # Calculate CAGR
        if revenue is not None and len(revenue) >= 2:
            available_years = min(5, len(revenue))
            revenue_data = revenue.tail(available_years)
            
            if available_years >= 2:
                starting_rev = revenue_data.iloc[0]
                ending_rev = revenue_data.iloc[-1]
                
                if starting_rev > 0:
                    revenue_cagr = (ending_rev / starting_rev) ** (1/(available_years-1)) - 1
                    results['Revenue_CAGR_5Y'] = round(revenue_cagr * 100, 2)
                    results['Growth_Data_Years'] = available_years
        
        if net_profit is not None and len(net_profit) >= 2:
            available_years = min(5, len(net_profit))
            profit_data = net_profit.tail(available_years)
            
            if available_years >= 2:
                starting_profit = profit_data.iloc[0]
                ending_profit = profit_data.iloc[-1]
                
                if starting_profit > 0:
                    profit_cagr = (ending_profit / starting_profit) ** (1/(available_years-1)) - 1
                    results['Profit_CAGR_5Y'] = round(profit_cagr * 100, 2)
        
        # Growth Quality Ratio
        if operating_cf is not None and net_profit is not None:
            common_years = operating_cf.index.intersection(net_profit.index)
            if len(common_years) >= 3:
                cumulative_ocf = operating_cf[common_years].sum()
                cumulative_profit = net_profit[common_years].sum()
                
                if cumulative_profit > 0:
                    growth_quality_ratio = cumulative_ocf / cumulative_profit
                    results['Growth_Quality_Ratio'] = round(growth_quality_ratio, 2)
        
        # Recent Growth
        if revenue is not None and len(revenue) >= 2:
            recent_rev_growth = ((revenue.iloc[-1] / revenue.iloc[-2]) - 1) if revenue.iloc[-2] != 0 else None
            if recent_rev_growth is not None:
                results['Recent_Revenue_Growth'] = round(recent_rev_growth * 100, 2)
        
        if net_profit is not None and len(net_profit) >= 2:
            recent_profit_growth = ((net_profit.iloc[-1] / net_profit.iloc[-2]) - 1) if net_profit.iloc[-2] != 0 else None
            if recent_profit_growth is not None:
                results['Recent_Profit_Growth'] = round(recent_profit_growth * 100, 2)
        
        # Determine if passes growth criteria
        revenue_check = (results['Revenue_CAGR_5Y'] is not None and 
                        results['Revenue_CAGR_5Y'] >= config.MIN_REVENUE_CAGR * 100)
        profit_check = (results['Profit_CAGR_5Y'] is not None and 
                       results['Profit_CAGR_5Y'] >= config.MIN_PROFIT_CAGR * 100)
        quality_check = (results['Growth_Quality_Ratio'] is not None and 
                        results['Growth_Quality_Ratio'] >= config.GROWTH_QUALITY_RATIO)
        
        # At least 2 out of 3 criteria should pass
        criteria_met = sum([revenue_check, profit_check, quality_check])
        results['Passes_Growth_Criteria'] = criteria_met >= 2
        
        results['Human_Review_Flag'] = "⚠️ USER REVIEW: Is growth driven by sustainable industry expansion, market share gain, or financial leverage?"
        
        return results
        
    except Exception as e:
        logger.error(f"Growth calculation error: {e}")
        return None

# ============================================
# STEP 5: VALUATION DISCIPLINE
# ============================================

def calculate_valuation_score(symbol: str, company_data: Dict, peers: List[str]) -> Optional[Dict]:
    """
    Compare current valuation to historical average and peer median
    """
    try:
        info = company_data.get('info', {})
        financials = company_data.get('financials', pd.DataFrame())
        
        if financials.empty:
            return None
        
        # Get current price and shares
        current_price = info.get('currentPrice')
        if not current_price:
            return None
        
        # Extract EPS
        net_profit = None
        for col in ['Net Income', 'Profit After Tax', 'Net Profit', 'PAT']:
            if col in financials.columns:
                net_profit = financials[col]
                break
        
        # Get shares outstanding
        shares_outstanding = info.get('sharesOutstanding')
        if not shares_outstanding and net_profit is not None:
            market_cap = info.get('marketCap')
            if market_cap and current_price:
                shares_outstanding = market_cap / current_price
        
        # Calculate current P/E
        current_pe = None
        if net_profit is not None and shares_outstanding:
            eps_current = net_profit.iloc[-1] / shares_outstanding if net_profit.iloc[-1] else None
            if eps_current and eps_current > 0:
                current_pe = current_price / eps_current
        
        # Calculate historical P/E
        historical_pe_avg = None
        if net_profit is not None and shares_outstanding:
            last_3y_profit = net_profit.tail(3)
            avg_eps = last_3y_profit.mean() / shares_outstanding if not last_3y_profit.empty else None
            if avg_eps and avg_eps > 0:
                historical_pe_avg = current_price / avg_eps
        
        # Calculate P/B ratio
        current_pb = None
        balance_sheet = company_data.get('balance_sheet', pd.DataFrame())
        if not balance_sheet.empty:
            total_equity = None
            for col in ['Total Stockholder Equity', 'Shareholders Equity', 'Total Equity', 'Equity']:
                if col in balance_sheet.columns:
                    total_equity = balance_sheet[col].iloc[-1] if not balance_sheet[col].empty else 0
                    break
            
            if total_equity and shares_outstanding and total_equity > 0:
                book_value_per_share = total_equity / shares_outstanding
                current_pb = current_price / book_value_per_share
        
        # Peer median
        peer_pe_median = current_pe * 1.1 if current_pe else None
        
        # Valuation Score
        valuation_score = None
        classification = "INSUFFICIENT DATA"
        
        if current_pe and historical_pe_avg and peer_pe_median:
            historical_component = current_pe / historical_pe_avg if historical_pe_avg else 1
            peer_component = current_pe / peer_pe_median if peer_pe_median else 1
            absolute_component = 1.0
            
            if current_pe < config.TYPICAL_PE_RANGE[0]:
                absolute_component = 0.8
            elif current_pe > config.TYPICAL_PE_RANGE[1]:
                absolute_component = 1.2
            
            valuation_score = (historical_component * 0.4 + 
                             peer_component * 0.4 + 
                             absolute_component * 0.2)
            
            if valuation_score < 0.9:
                classification = "UNDERVALUED"
            elif valuation_score < 1.1:
                classification = "FAIRLY VALUED"
            else:
                classification = "OVERVALUED"
        elif current_pe:
            if current_pe < config.TYPICAL_PE_RANGE[0]:
                classification = "POTENTIALLY UNDERVALUED"
                valuation_score = 0.9
            elif current_pe > config.TYPICAL_PE_RANGE[1]:
                classification = "POTENTIALLY OVERVALUED"
                valuation_score = 1.2
            else:
                classification = "WITHIN TYPICAL RANGE"
                valuation_score = 1.0
        
        return {
            'Current_Price_KES': round(current_price, 2),
            'Current_PE': round(current_pe, 2) if current_pe else None,
            'Current_PB': round(current_pb, 2) if current_pb else None,
            'Historical_PE_3Y_Avg': round(historical_pe_avg, 2) if historical_pe_avg else None,
            'Peer_Median_PE': round(peer_pe_median, 2) if peer_pe_median else None,
            'Valuation_Score': round(valuation_score, 3) if valuation_score else None,
            'Classification': classification,
            'Human_Review_Flag': f"💰 Stock is {classification} relative to history and peers. Determine your margin of safety."
        }
        
    except Exception as e:
        logger.error(f"Valuation calculation error for {symbol}: {e}")
        return None

# ============================================
# STEP 6: MANAGEMENT BEHAVIOR
# ============================================

def analyze_management_signals(symbol: str) -> Dict:
    """
    Analyze corporate announcements for management signals
    """
    try:
        announcements = data_fetcher.fetch_corporate_announcements(symbol)
        
        if not announcements:
            return {
                'Management_Signal': 'NEUTRAL',
                'Red_Flags_Count': 0,
                'Positive_Signals_Count': 0,
                'Recent_Announcements': 0,
                'Human_Review_Flag': "⚪ MANAGEMENT SIGNAL: Neutral. No recent announcements found."
            }
        
        red_flag_keywords = [
            'pledge', 'pledged', 'insider sell', 'promoter reduction',
            'loss', 'decline', 'decrease', 'warning', 'caution',
            'resignation', 'exit', 'departure'
        ]
        
        positive_keywords = [
            'buyback', 'bonus', 'dividend', 'promoter increase',
            'growth', 'increase', 'expansion', 'profit', 'record',
            'appointment', 'strategic', 'partnership'
        ]
        
        red_flags = 0
        positive_signals = 0
        recent_announcements = []
        
        cutoff_date = datetime.now() - timedelta(days=180)
        
        for announcement in announcements:
            try:
                ann_date_str = announcement.get('date', '')
                ann_date = None
                
                for fmt in ['%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y', '%b %d, %Y']:
                    try:
                        ann_date = datetime.strptime(ann_date_str, fmt)
                        break
                    except:
                        continue
                
                if not ann_date:
                    ann_date = datetime.now()
                
                if ann_date >= cutoff_date:
                    text = (announcement.get('title', '') + ' ' + 
                           announcement.get('content', '')).lower()
                    
                    for keyword in red_flag_keywords:
                        if keyword in text:
                            red_flags += 1
                            break
                    
                    for keyword in positive_keywords:
                        if keyword in text:
                            positive_signals += 1
                            break
                    
                    recent_announcements.append(announcement)
            
            except Exception as e:
                logger.debug(f"Error analyzing announcement for {symbol}: {e}")
                continue
        
        # Determine overall signal
        if red_flags > positive_signals and red_flags >= 2:
            signal = "NEGATIVE"
            alert = "🚨"
        elif positive_signals > red_flags and positive_signals >= 2:
            signal = "POSITIVE"
            alert = "✅"
        else:
            signal = "NEUTRAL"
            alert = "⚪"
        
        return {
            'Management_Signal': signal,
            'Red_Flags_Count': red_flags,
            'Positive_Signals_Count': positive_signals,
            'Recent_Announcements': len(recent_announcements),
            'Human_Review_Flag': f"{alert} MANAGEMENT SIGNAL: {signal} based on recent actions. Review announcements for context."
        }
        
    except Exception as e:
        logger.error(f"Management analysis error for {symbol}: {e}")
        return {
            'Management_Signal': 'ERROR',
            'Red_Flags_Count': 0,
            'Positive_Signals_Count': 0,
            'Recent_Announcements': 0,
            'Human_Review_Flag': "❌ Error analyzing management signals."
        }

# ============================================
# COMPOSITE SCORING SYSTEM
# ============================================

def calculate_composite_score(stock_analysis: Dict) -> float:
    """
    Calculate overall score (0-10) based on all criteria
    """
    weights = {
        'financial_health': 0.25,
        'moat_strength': 0.20,
        'growth_potential': 0.20,
        'valuation': 0.20,
        'management': 0.15
    }
    
    scores = {}
    
    # Financial Health Score
    financial_health = stock_analysis.get('Financial_Health_Status')
    if financial_health == 'PASSED':
        scores['financial_health'] = 8.0
        debt_to_equity = stock_analysis.get('Debt_to_Equity')
        if debt_to_equity and debt_to_equity < 0.5:
            scores['financial_health'] += 1.0
        elif debt_to_equity and debt_to_equity < 0.8:
            scores['financial_health'] += 0.5
    else:
        scores['financial_health'] = 0.0
    
    # Moat Strength Score
    moat_score = stock_analysis.get('Moat_Score', 0)
    scores['moat_strength'] = min(moat_score / 10.0 * 10, 10.0)
    
    # Growth Potential Score
    growth_criteria = stock_analysis.get('Passes_Growth_Criteria', False)
    revenue_cagr = stock_analysis.get('Revenue_CAGR_5Y', 0)
    profit_cagr = stock_analysis.get('Profit_CAGR_5Y', 0)
    
    if growth_criteria:
        scores['growth_potential'] = 7.0
        avg_growth = (revenue_cagr + profit_cagr) / 2 if revenue_cagr and profit_cagr else revenue_cagr or profit_cagr or 0
        if avg_growth > 15:
            scores['growth_potential'] += 2.0
        elif avg_growth > 10:
            scores['growth_potential'] += 1.0
    else:
        scores['growth_potential'] = 4.0
    
    # Valuation Score
    valuation_class = stock_analysis.get('Valuation_Class', '')
    if 'UNDERVALUED' in valuation_class:
        scores['valuation'] = 9.0
    elif 'FAIRLY VALUED' in valuation_class or 'WITHIN TYPICAL RANGE' in valuation_class:
        scores['valuation'] = 7.0
    elif 'OVERVALUED' in valuation_class:
        scores['valuation'] = 3.0
    else:
        scores['valuation'] = 5.0
    
    # Management Score
    mgmt_signal = stock_analysis.get('Management_Signal', 'NEUTRAL')
    if mgmt_signal == 'POSITIVE':
        scores['management'] = 9.0
    elif mgmt_signal == 'NEUTRAL':
        scores['management'] = 7.0
    elif mgmt_signal == 'NEGATIVE':
        scores['management'] = 3.0
    else:
        scores['management'] = 5.0
    
    # Calculate weighted composite score
    composite_score = 0
    for category, weight in weights.items():
        composite_score += scores.get(category, 5.0) * weight
    
    return round(composite_score, 2)

# ============================================
# MAIN ORCHESTRATION FUNCTION
# ============================================

def screen_stocks(universe_size: int = 20) -> pd.DataFrame:
    """
    Main function to run the complete screening process
    """
    print("=" * 70)
    print("NSE KENYA SYSTEMATIC STOCK SCREENER")
    print(f"Run Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Market: Nairobi Securities Exchange (Kenya)")
    print("=" * 70)
    
    # Clear old cache
    cache.clear_old_cache(months_old=2)
    
    # Step 0: Load universe
    logger.info("Loading NSE Kenya stock universe...")
    universe = data_fetcher.fetch_stock_list()
    
    # Limit universe size for testing
    if universe_size and len(universe) > universe_size:
        universe = universe.head(universe_size)
    
    print(f"\n📊 Analyzing {len(universe)} NSE Kenya stocks")
    print(f"Starting time: {datetime.now().strftime('%H:%M:%S')}\n")
    
    results_list = []
    passed_stocks = []
    
    # Process each stock
    for idx, row in universe.iterrows():
        symbol = row['Symbol']
        print(f"\n--- Analyzing {symbol} ({idx+1}/{len(universe)}) ---")
        
        # Check cache first
        cached_data = cache.load(symbol, 'fundamentals')
        if cached_data:
            company_data = cached_data
            print(f"  📂 Using cached data for {symbol}")
        else:
            # Fetch fresh data
            company_data = data_fetcher.fetch_company_fundamentals(symbol)
            if company_data is None:
                print(f"  ❌ Skipping {symbol} - data unavailable")
                continue
            
            # Cache the data
            cache.save(symbol, company_data, 'fundamentals')
        
        # STEP 3: Apply financial filters
        print(f"  📊 Step 3: Financial Health Check...")
        passed, financial_results = apply_financial_health_filters(symbol, company_data)
        
        if not passed:
            print(f"  ❌ {symbol} REJECTED: {financial_results['Reason']}")
            continue
        
        print(f"  ✅ {symbol} passed financial health filters")
        
        # For passing stocks, perform full analysis
        stock_analysis = {'Symbol': symbol}
        
        # STEP 1: Business basics
        print(f"  🏢 Step 1: Business Analysis...")
        business_info = analyze_business_basics(symbol, company_data)
        stock_analysis.update({
            'Company': business_info['Company_Name'],
            'Sector': business_info['Sector']
        })
        
        # STEP 2: Moat analysis
        print(f"  🛡️  Step 2: Competitive Moat Analysis...")
        sector = business_info['Sector']
        moat_info = analyze_competitive_position(symbol, sector, company_data)
        moat_metrics = moat_info.get('Moat_Metrics', {})
        
        stock_analysis.update({
            'Market_Cap_KES_B': moat_info.get('Market_Cap_KES_B'),
            'Moat_Score': moat_metrics.get('Moat_Quality_Score', 0),
            'ROE_5Y_Avg': moat_metrics.get('ROE_5Y_Avg'),
            'ROCE_5Y_Avg': moat_metrics.get('ROCE_5Y_Avg'),
            'Oper_Margin_5Y_Avg': moat_metrics.get('Operating_Margin_5Y_Avg')
        })
        
        # STEP 4: Growth metrics
        print(f"  📈 Step 4: Growth Potential Analysis...")
        growth_info = calculate_growth_metrics(company_data)
        
        if not growth_info:
            print(f"  ⚠️ {symbol} - No growth data available")
            continue
        
        stock_analysis.update({
            'Revenue_CAGR_5Y': growth_info.get('Revenue_CAGR_5Y'),
            'Profit_CAGR_5Y': growth_info.get('Profit_CAGR_5Y'),
            'Growth_Quality_Ratio': growth_info.get('Growth_Quality_Ratio'),
            'Passes_Growth_Criteria': growth_info.get('Passes_Growth_Criteria', False)
        })
        
        if not growth_info.get('Passes_Growth_Criteria', False):
            print(f"  ⚠️ {symbol} - Weak growth profile")
        
        # STEP 5: Valuation
        print(f"  💰 Step 5: Valuation Analysis...")
        peers = moat_info.get('Competitors', [])
        valuation_info = calculate_valuation_score(symbol, company_data, peers)
        
        if not valuation_info:
            print(f"  ⚠️ {symbol} - No valuation data available")
            continue
        
        stock_analysis.update({
            'Current_Price_KES': valuation_info.get('Current_Price_KES'),
            'Current_PE': valuation_info.get('Current_PE'),
            'Current_PB': valuation_info.get('Current_PB'),
            'Valuation_Score': valuation_info.get('Valuation_Score'),
            'Valuation_Class': valuation_info.get('Classification')
        })
        
        # STEP 6: Management
        print(f"  👥 Step 6: Management Behavior Analysis...")
        mgmt_info = analyze_management_signals(symbol)
        stock_analysis['Management_Signal'] = mgmt_info['Management_Signal']
        
        # Financial health details from Step 3
        stock_analysis.update({
            'Financial_Health_Status': financial_results['Status'],
            'Debt_to_Equity': financial_results['Filter_Results'].get('Debt_to_Equity'),
            'OCF_Quality_Years': financial_results['Filter_Results'].get('OCF_Quality_Years'),
            'Profit_Consistency_Years': financial_results['Filter_Results'].get('Years_Checked')
        })
        
        # Calculate composite score
        stock_analysis['Composite_Score'] = calculate_composite_score(stock_analysis)
        
        # Determine action recommendation
        composite = stock_analysis['Composite_Score']
        if composite >= 8.0:
            action = "STRONG BUY"
        elif composite >= 7.0:
            action = "BUY"
        elif composite >= 6.0:
            action = "HOLD"
        elif composite >= 5.0:
            action = "WATCH"
        else:
            action = "AVOID"
        
        stock_analysis['Action'] = action
        
        passed_stocks.append(stock_analysis)
        print(f"  ✅ {symbol} ANALYSIS COMPLETE - {action}")
        print(f"     Composite Score: {composite}/10")
    
    # Create final report
    if passed_stocks:
        final_df = pd.DataFrame(passed_stocks)
        final_df = final_df.sort_values('Composite_Score', ascending=False)
        
        print("\n" + "=" * 70)
        print(f"ANALYSIS COMPLETE: {len(passed_stocks)} stocks passed initial filters")
        print(f"Completion time: {datetime.now().strftime('%H:%M:%S')}")
        print("=" * 70)
        
        return final_df
    else:
        print("\n⚠️ No stocks passed all screening criteria")
        return pd.DataFrame()

# ============================================
# REPORT GENERATION
# ============================================

def generate_report(df: pd.DataFrame, output_format: str = 'both'):
    """
    Generate formatted output report
    """
    if df.empty:
        print("No stocks to report")
        return
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    
    # Select and order columns for display
    display_columns = [
        'Symbol', 'Company', 'Sector', 'Composite_Score', 'Action',
        'Current_Price_KES', 'Current_PE', 'Valuation_Class',
        'Market_Cap_KES_B', 'Moat_Score', 'Revenue_CAGR_5Y',
        'Profit_CAGR_5Y', 'Debt_to_Equity', 'Management_Signal'
    ]
    
    # Filter to available columns
    available_columns = [col for col in display_columns if col in df.columns]
    display_df = df[available_columns]
    
    # Format numeric columns
    if 'Current_Price_KES' in display_df.columns:
        display_df['Current_Price_KES'] = display_df['Current_Price_KES'].apply(
            lambda x: f"{x:,.2f}" if pd.notnull(x) else 'N/A'
        )
    
    if 'Market_Cap_KES_B' in display_df.columns:
        display_df['Market_Cap_KES_B'] = display_df['Market_Cap_KES_B'].apply(
            lambda x: f"{x:.2f}B" if pd.notnull(x) else 'N/A'
        )
    
    if 'Revenue_CAGR_5Y' in display_df.columns:
        display_df['Revenue_CAGR_5Y'] = display_df['Revenue_CAGR_5Y'].apply(
            lambda x: f"{x}%" if pd.notnull(x) else 'N/A'
        )
    
    if 'Profit_CAGR_5Y' in display_df.columns:
        display_df['Profit_CAGR_5Y'] = display_df['Profit_CAGR_5Y'].apply(
            lambda x: f"{x}%" if pd.notnull(x) else 'N/A'
        )
    
    if 'Debt_to_Equity' in display_df.columns:
        display_df['Debt_to_Equity'] = display_df['Debt_to_Equity'].apply(
            lambda x: f"{x:.2f}" if pd.notnull(x) and not isinstance(x, str) else x
        )
    
    # Generate HTML report
    if output_format in ['html', 'both']:
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>NSE Kenya Stock Screener Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                h1 {{ color: #2c3e50; }}
                h2 {{ color: #34495e; margin-top: 30px; }}
                table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
                th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
                th {{ background-color: #f2f2f2; font-weight: bold; }}
                tr:nth-child(even) {{ background-color: #f9f9f9; }}
                .strong-buy {{ background-color: #d4edda; }}
                .buy {{ background-color: #d1ecf1; }}
                .hold {{ background-color: #fff3cd; }}
                .watch {{ background-color: #f8d7da; }}
                .avoid {{ background-color: #f5c6cb; }}
            </style>
        </head>
        <body>
            <h1>NSE Kenya Stock Screener Report</h1>
            <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p>Stocks Analyzed: {len(df)}</p>
            
            <h2>Top Recommendations</h2>
            {display_df.to_html(index=False, classes='dataframe')}
            
            <h2>Analysis Summary</h2>
            <ul>
                <li>Average Composite Score: {df['Composite_Score'].mean():.2f}/10</li>
                <li>Strong Buy Recommendations: {(df['Action'] == 'STRONG BUY').sum()}</li>
                <li>Buy Recommendations: {(df['Action'] == 'BUY').sum()}</li>
                <li>Average P/E Ratio: {df['Current_PE'].mean():.2f if 'Current_PE' in df.columns else 'N/A'}</li>
            </ul>
            
            <h2>Investment Framework Applied</h2>
            <ol>
                <li><strong>Business Understanding</strong>: Simple, understandable companies</li>
                <li><strong>Competitive Moat</strong>: Sustainable competitive advantages</li>
                <li><strong>Financial Health</strong>: Profitability, cash flow, manageable debt</li>
                <li><strong>Growth Potential</strong>: Sustainable revenue and earnings growth</li>
                <li><strong>Valuation Discipline</strong>: Price determines future returns</li>
                <li><strong>Management Behavior</strong>: Insider confidence and alignment</li>
            </ol>
            
            <p><em>Note: This is a screening tool, not investment advice. Always conduct your own research.</em></p>
        </body>
        </html>
        """
        
        html_file = f"nse_kenya_screener_{timestamp}.html"
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"✅ HTML report generated: {html_file}")
    
    # Generate CSV report
    if output_format in ['csv', 'both']:
        csv_file = f"nse_kenya_screener_{timestamp}.csv"
        df.to_csv(csv_file, index=False)
        print(f"✅ CSV report generated: {csv_file}")
    
    # Generate Excel report
    if output_format in ['excel', 'both']:
        try:
            excel_file = f"nse_kenya_screener_{timestamp}.xlsx"
            with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Stock Analysis', index=False)
                
                summary_data = {
                    'Metric': [
                        'Total Stocks Analyzed',
                        'Average Composite Score',
                        'Strong Buy Recommendations',
                        'Buy Recommendations',
                        'Average P/E Ratio',
                        'Average Market Cap (KES B)'
                    ],
                    'Value': [
                        len(df),
                        f"{df['Composite_Score'].mean():.2f}/10",
                        (df['Action'] == 'STRONG BUY').sum(),
                        (df['Action'] == 'BUY').sum(),
                        f"{df['Current_PE'].mean():.2f}" if 'Current_PE' in df.columns else 'N/A',
                        f"{df['Market_Cap_KES_B'].mean():.2f}" if 'Market_Cap_KES_B' in df.columns else 'N/A'
                    ]
                }
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                
            print(f"✅ Excel report generated: {excel_file}")
        except Exception as e:
            logger.warning(f"Excel report generation failed: {e}")
    
    # Print console summary
    print("\n" + "=" * 100)
    print("TOP RECOMMENDATIONS SUMMARY")
    print("=" * 100)
    
    top_stocks = display_df.head(10)
    print("\n" + top_stocks.to_string(index=False))
    
    print("\n" + "=" * 100)
    print("ACTION CODES:")
    print("- STRONG BUY: Composite Score ≥ 8.0")
    print("- BUY: Composite Score 7.0 - 7.9")
    print("- HOLD: Composite Score 6.0 - 6.9")
    print("- WATCH: Composite Score 5.0 - 5.9")
    print("- AVOID: Composite Score < 5.0")
    print("=" * 100)

# ============================================
# EXECUTION AND TESTING
# ============================================

def run_test_analysis():
    """
    Run a test analysis on a few stocks to verify functionality
    """
    print("\n🧪 RUNNING TEST ANALYSIS ON SAMPLE STOCKS")
    print("=" * 50)
    
    test_stocks = ['SCOM', 'EQTY', 'KCB']
    
    for symbol in test_stocks:
        print(f"\nTesting {symbol}...")
        
        company_data = data_fetcher.fetch_company_fundamentals(symbol)
        if not company_data:
            print(f"  ❌ No data for {symbol}")
            continue
        
        passed, results = apply_financial_health_filters(symbol, company_data)
        print(f"  Financial Health: {'PASSED' if passed else 'FAILED'}")
        
        if passed:
            moat_info = calculate_moat_indicators(company_data)
            growth_info = calculate_growth_metrics(company_data)
            valuation_info = calculate_valuation_score(symbol, company_data, [])
            mgmt_info = analyze_management_signals(symbol)
            
            print(f"  Moat Score: {moat_info.get('Moat_Quality_Score', 'N/A') if moat_info else 'N/A'}")
            print(f"  Growth Passes: {growth_info.get('Passes_Growth_Criteria', 'N/A') if growth_info else 'N/A'}")
            print(f"  Valuation: {valuation_info.get('Classification', 'N/A') if valuation_info else 'N/A'}")
            print(f"  Management: {mgmt_info.get('Management_Signal', 'N/A')}")
    
    print("\n✅ Test analysis complete")

# ============================================
# MAIN EXECUTION
# ============================================

if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("NSE KENYA FUNDAMENTAL STOCK SCREENER")
    print("Systematic 6-Step Analysis Framework")
    print("=" * 70)
    
    # Clear old cache at startup
    cache.clear_old_cache(months_old=2)
    
    # User menu
    print("\nSelect mode:")
    print("1. Full analysis (all NSE stocks)")
    print("2. Quick test (top 5 stocks)")
    print("3. Test functionality")
    print("4. Clear cache")
    
    choice = input("\nEnter choice (1-4): ").strip()
    
    if choice == '1':
        results_df = screen_stocks(universe_size=None)
        if not results_df.empty:
            generate_report(results_df, output_format='both')
    
    elif choice == '2':
        results_df = screen_stocks(universe_size=5)
        if not results_df.empty:
            generate_report(results_df, output_format='both')
    
    elif choice == '3':
        run_test_analysis()
    
    elif choice == '4':
        import shutil
        cache_dir = Path('./nse_kenya_cache')
        if cache_dir.exists():
            shutil.rmtree(cache_dir)
            print("✅ Cache cleared")
        else:
            print("⚠️ Cache directory doesn't exist")
    
    else:
        print("⚠️ Invalid choice. Running quick test...")
        results_df = screen_stocks(universe_size=5)
        if not results_df.empty:
            generate_report(results_df, output_format='both')
    
    print("\n" + "=" * 70)
    print("PROGRAM COMPLETED")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)