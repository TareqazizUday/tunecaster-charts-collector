import asyncio
import json
import re
import os
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin

class TuneCasterCompleteScraper:
    def __init__(self):
        self.base_url = "https://tunecaster.com"
        self.pop_urls = []
        self.rock_urls = []
        self.all_chart_data = []
        self.progress_file = 'data/scraper_progress.json'
        self.processed_urls = set()
    
    def load_progress(self):
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    progress = json.load(f)
                    self.processed_urls = set(progress.get('processed_urls', []))
                    print(f"Loaded progress: {len(self.processed_urls)} URLs already processed")
                    return True
            except Exception as e:
                print(f"Could not load progress: {e}")
        return False
    
    def save_progress(self, current_url):
        try:
            os.makedirs('data', exist_ok=True)
            self.processed_urls.add(current_url)
            progress = {
                'processed_urls': list(self.processed_urls),
                'last_processed': current_url,
                'timestamp': datetime.now().isoformat()
            }
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(progress, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"Could not save progress: {e}")
    
    async def discover_all_chart_urls(self):
        print("Discovering all chart URLs...")
        
        decade_pages = {
            'pop': [
                'https://tunecaster.com/chart0.html',
                'https://tunecaster.com/chart1.html',
                'https://tunecaster.com/chart6.html',
                'https://tunecaster.com/chart7.html',  
                'https://tunecaster.com/chart8.html',
                'https://tunecaster.com/chart9.html',
            ],
            'rock': [
                'https://tunecaster.com/chart1.html',
                'https://tunecaster.com/rock0.html',
                'https://tunecaster.com/rock8.html',
                'https://tunecaster.com/rock9.html', 
            ]
        }
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()
            
            try:
                print("Discovering Pop chart URLs...")
                for decade_url in decade_pages['pop']:
                    urls = await self.extract_urls_from_decade_page(page, decade_url, 'pop')
                    self.pop_urls.extend(urls)
                    print(f"Found {len(urls)} pop URLs from {decade_url}")
                    
                    pop_2010_urls = [url for url in urls if '/charts/10/' in url]
                    if pop_2010_urls:
                        print(f"  -> {len(pop_2010_urls)} URLs for 2010 from {decade_url}")
                    
                    await asyncio.sleep(1)
                
                print("Discovering Rock chart URLs...")
                for decade_url in decade_pages['rock']:
                    urls = await self.extract_urls_from_decade_page(page, decade_url, 'rock')
                    self.rock_urls.extend(urls)
                    print(f"Found {len(urls)} rock URLs from {decade_url}")
                    
                    rock_2010_urls = [url for url in urls if '/charts/10/' in url]
                    if rock_2010_urls:
                        print(f"  -> {len(rock_2010_urls)} URLs for 2010 from {decade_url}")
                    
                    await asyncio.sleep(1)
                
            finally:
                await browser.close()
        
        # Remove duplicates first
        self.pop_urls = list(set(self.pop_urls))
        self.rock_urls = list(set(self.rock_urls))
        
        # Sort pop URLs to prioritize 2010s (decade 10) first
        def sort_pop_urls(url):
            match = re.search(r'/charts/(\d{2})/week(\d{4})\.html', url)
            if match:
                decade = int(match.group(1))
                week = match.group(2)
                if decade == 10:
                    return (0, decade, week)  # 2010s first
                else:
                    return (1, decade, week)  # Other decades after
            return (2, 99, "9999")  # Invalid URLs last
        
        self.pop_urls = sorted(self.pop_urls, key=sort_pop_urls)
        
        # Sort rock URLs to prioritize 2010s (decade 10) first
        def sort_rock_urls(url):
            match = re.search(r'/charts/(\d{2})/rock(\d{4})\.html', url)
            if match:
                decade = int(match.group(1))
                week = match.group(2)
                if decade == 10:
                    return (0, decade, week)  # 2010s first
                else:
                    return (1, decade, week)  # Other decades after
            return (2, 99, "9999")  # Invalid URLs last
        
        self.rock_urls = sorted(self.rock_urls, key=sort_rock_urls)
        
        # Show 2010 URLs count for both pop and rock
        pop_2010_urls = [url for url in self.pop_urls if '/charts/10/' in url]
        rock_2010_urls = [url for url in self.rock_urls if '/charts/10/' in url]
        
        print(f"\n2010 CHARTS SUMMARY:")
        print(f"Rock 2010 URLs: {len(rock_2010_urls)}")
        if rock_2010_urls:
            print("First 10 2010 rock URLs:")
            for i, url in enumerate(rock_2010_urls[:10], 1):
                print(f"  {i:2d}. {url}")
            if len(rock_2010_urls) > 10:
                print(f"  ... and {len(rock_2010_urls) - 10} more")
        
        print(f"\nPop 2010 URLs: {len(pop_2010_urls)}")
        if pop_2010_urls:
            print("First 10 2010 pop URLs:")
            for i, url in enumerate(pop_2010_urls[:10], 1):
                print(f"  {i:2d}. {url}")
            if len(pop_2010_urls) > 10:
                print(f"  ... and {len(pop_2010_urls) - 10} more")
        
        print(f"\nURL Discovery Complete:")
        print(f"Rock Charts: {len(self.rock_urls)} (2010: {len(rock_2010_urls)})")
        print(f"Pop Charts: {len(self.pop_urls)} (2010: {len(pop_2010_urls)})")
        print(f"Total Charts: {len(self.pop_urls) + len(self.rock_urls)}")
        print(f"Total 2010 Charts: {len(pop_2010_urls) + len(rock_2010_urls)}")
        
        print(f"\nPROCESSING ORDER:")
        print(f"1. ALL ROCK CHARTS ({len(self.rock_urls)}) - 2010 first")
        print(f"2. ALL POP CHARTS ({len(self.pop_urls)}) - 2010 first")
    
    async def extract_urls_from_decade_page(self, page, decade_url, chart_type):
        urls = []
        try:
            await page.goto(decade_url, timeout=30000)
            await page.wait_for_timeout(2000)
            
            links = await page.evaluate('''
                () => {
                    const links = Array.from(document.querySelectorAll('a[href]'));
                    return links.map(link => link.href);
                }
            ''')
            
            for link in links:
                if self.is_chart_url(link, chart_type):
                    if link.startswith('/'):
                        link = urljoin(self.base_url, link)
                    urls.append(link)
            
        except Exception as e:
            print(f"Error extracting from {decade_url}: {e}")
        
        return urls
    
    def is_chart_url(self, url, chart_type):
        if chart_type == 'pop':
            return bool(re.search(r'/charts/[0-9]+/week[0-9]+\.html', url))
        elif chart_type == 'rock':
            return bool(re.search(r'/charts/[0-9]+/rock[0-9]+\.html', url))
        return False
    
    async def scrape_single_chart(self, url, chart_type):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            page = await context.new_page()
            
            try:
                await page.goto(url, wait_until='networkidle', timeout=30000)
                await page.wait_for_timeout(3000)
                
                html_content = await page.content()
                chart_data = self.parse_chart(html_content, url, chart_type)
                
                if chart_data and len(chart_data['records']) < 10:
                    chart_data = await self.parse_chart_alternative(page, url, chart_type)
                
                return chart_data
                
            except Exception as e:
                print(f"Error scraping {url}: {e}")
                return None
            finally:
                await browser.close()
    
    async def parse_chart_alternative(self, page, url, chart_type):
        html_content = await page.content()
        soup = BeautifulSoup(html_content, 'html.parser')
        chart_date = self.extract_chart_date_from_page(soup)
        
        if chart_date is None:
            chart_date = self.extract_chart_date_from_url(url)
            
        if chart_date is None:
            print(f"Skipping alternative parsing due to invalid date: {url}")
            return None
        
        try:
            chart_data = await page.evaluate('''
                () => {
                    const songs = [];
                    const positionCells = document.querySelectorAll('td.thisWeek');
                    
                    positionCells.forEach(posCell => {
                        const posText = posCell.textContent.trim();
                        if (posText && posText !== 'TW' && /^\\d+$/.test(posText)) {
                            const position = parseInt(posText);
                            let title = '';
                            let artist = '';
                            
                            const row = posCell.closest('tr');
                            if (row) {
                                const titleCell = row.querySelector('td.title20') || row.querySelector('td.titleBoth20');
                                if (titleCell) {
                                    const titleLink = titleCell.querySelector('a.songLink');
                                    title = titleLink ? titleLink.textContent.trim() : titleCell.textContent.trim();
                                }
                            }
                            
                            let currentTable = posCell.closest('table');
                            let nextTable = currentTable ? currentTable.nextElementSibling : null;
                            let attempts = 0;
                            let foundArtist = false;
                            
                            while (nextTable && attempts < 15 && !foundArtist) {
                                if (nextTable.tagName === 'TABLE') {
                                    const artistCell = nextTable.querySelector('td.artist20');
                                    if (artistCell) {
                                        const artistLinks = artistCell.querySelectorAll('a.artistLink');
                                        if (artistLinks.length > 1) {
                                            const artistNames = [];
                                            artistLinks.forEach(link => {
                                                const name = link.textContent.trim();
                                                if (name) artistNames.push(name);
                                            });
                                            if (artistNames.length > 0) {
                                                artist = artistNames.join(' with ');
                                                foundArtist = true;
                                                break;
                                            }
                                        } else if (artistLinks.length === 1) {
                                            artist = artistLinks[0].textContent.trim();
                                            if (artist) {
                                                foundArtist = true;
                                                break;
                                            }
                                        } else {
                                            artist = artistCell.textContent.trim();
                                            if (artist) {
                                                foundArtist = true;
                                                break;
                                            }
                                        }
                                    }
                                    
                                    const allCells = nextTable.querySelectorAll('td');
                                    for (let cell of allCells) {
                                        const cellText = cell.textContent.trim();
                                        if (cellText && cellText.length > 1 && cellText.length < 200 &&
                                            /[a-zA-Z]/.test(cellText) &&
                                            !cellText.includes('download') &&
                                            !cellText.includes('amazon') &&
                                            !cellText.includes('http') &&
                                            !cellText.includes('../../') &&
                                            !cellText.match(/^\\d+$/) &&
                                            !cellText.includes('week') &&
                                            !cellText.includes('chart') &&
                                            !cellText.startsWith('[') &&
                                            cellText !== '|') {
                                            
                                            if (!artist || artist.length < cellText.length) {
                                                artist = cellText;
                                            }
                                        }
                                    }
                                    
                                    if (artist) {
                                        foundArtist = true;
                                        break;
                                    }
                                }
                                nextTable = nextTable.nextElementSibling;
                                attempts++;
                            }
                            
                            if (title) {
                                songs.push({
                                    position: position,
                                    title: title,
                                    artist: artist || ''
                                });
                            }
                        }
                    });
                    
                    return songs;
                }
            ''')
            
            if chart_data:
                chart_data.sort(key=lambda x: x.get('position', 999))
                
                for song in chart_data:
                    if song.get('artist'):
                        song['artist'] = self.parse_multiple_artists(song['artist'])
                    else:
                        song['artist'] = []
                
                return {
                    'chart_info': {
                        'chart_type': chart_type,
                        'chart_date': chart_date,
                        'url': url
                    },
                    'records': [
                        {
                            "id": self.generate_record_id(url, song['position']),
                            "chart_date": chart_date,
                            "rank": song['position'],
                            "title": song['title'],
                            "artist": json.dumps(song['artist']),
                            "url": url
                        }
                        for song in chart_data
                    ]
                }
            
        except Exception as e:
            print(f"Alternative parsing failed: {e}")
        
        return None
    
    def parse_chart(self, html_content, url, chart_type):
        soup = BeautifulSoup(html_content, 'html.parser')
        songs = self.extract_songs_from_html(soup)
        
        chart_date = self.extract_chart_date_from_page(soup)
        
        if chart_date is None:
            chart_date = self.extract_chart_date_from_url(url)
            
        if chart_date is None:
            print(f"Skipping chart due to invalid date: {url}")
            return None
            
        records = []
        
        for song in songs:
            record = {
                "id": self.generate_record_id(url, song['position']),
                "chart_date": chart_date,
                "rank": song['position'],
                "title": song['title'],
                "artist": json.dumps(song['artist'], ensure_ascii=False),
                "url": url
            }
            records.append(record)
        
        return {
            'chart_info': {
                'chart_type': chart_type,
                'chart_date': chart_date,
                'url': url
            },
            'records': records
        }
    
    def extract_songs_from_html(self, soup):
        songs = []
        songs.extend(self.extract_using_table_structure(soup))
        
        text_songs = self.extract_using_sequential_parsing(soup)
        for song in text_songs:
            if not any(s['position'] == song['position'] for s in songs):
                songs.append(song)
        
        unique_songs = self.clean_songs(songs)
        unique_songs.sort(key=lambda x: x.get('position', 999))
        
        return unique_songs
    
    def extract_using_table_structure(self, soup):
        songs = []
        tables = soup.find_all('table', class_='t2')
        
        i = 0
        while i < len(tables):
            table = tables[i]
            tw_cell = table.find('td', class_='thisWeek')
            title_cell = table.find('td', class_='title20') or table.find('td', class_='titleBoth20')
            
            if tw_cell and title_cell:
                tw_text = tw_cell.get_text().strip()
                
                if tw_text == 'TW' or not tw_text.isdigit():
                    i += 1
                    continue
                
                tw_position = int(tw_text)
                title = self.extract_title_from_cell(title_cell)
                artist = self.find_artist_in_next_tables(tables, i + 1)
                
                if title:
                    if not isinstance(artist, str):
                        artist = str(artist) if artist else ""
                    
                    artists = self.parse_multiple_artists(artist)
                    songs.append({
                        'position': tw_position,
                        'title': title,
                        'artist': artists
                    })
            
            i += 1
        
        return songs
    
    def extract_using_sequential_parsing(self, soup):
        songs = []
        page_text = soup.get_text()
        lines = [line.strip() for line in page_text.split('\n') if line.strip()]
        
        i = 0
        while i < len(lines):
            line = lines[i]
            
            if any(skip in line.lower() for skip in ['download', 'amazon', 'img', 'src=', 'http', '![]']):
                i += 1
                continue
            
            tw_peaks_match = re.search(r'\[TW\]peaks.*?\[(?:rock|pop)\].*?(\d+)\s*\|\s*(\d+)\s*\|\s*([^|]+)', line)
            if tw_peaks_match:
                tw_position = int(tw_peaks_match.group(2))
                title = tw_peaks_match.group(3).strip()
                artist = self.find_artist_in_text_lines(lines, i + 1)
                
                if title:
                    if not isinstance(artist, str):
                        artist = str(artist) if artist else ""
                    
                    artists = self.parse_multiple_artists(artist)
                    songs.append({
                        'position': tw_position,
                        'title': title,
                        'artist': artists
                    })
                i += 1
                continue
            
            standard_match = re.match(r'^(\d+)\s*\|\s*(\d+)\s*\|\s*([^|]*)', line)
            if standard_match:
                tw_position = int(standard_match.group(2))
                title = standard_match.group(3).strip()
                
                if not title or len(title.strip()) < 2:
                    title = self.find_title_in_next_lines(lines, i + 1)
                
                artist = self.find_artist_in_text_lines(lines, i + 1)
                
                if title and len(title.strip()) >= 2:
                    if not isinstance(artist, str):
                        artist = str(artist) if artist else ""
                    
                    artists = self.parse_multiple_artists(artist)
                    songs.append({
                        'position': tw_position,
                        'title': title,
                        'artist': artists
                    })
                i += 1
                continue
            
            new_entry_match = re.match(r'^\-\s*\|\s*(\d+)\s*\|\s*([^|]+)', line)
            if new_entry_match:
                tw_position = int(new_entry_match.group(1))
                title = new_entry_match.group(2).strip()
                artist = self.find_artist_in_text_lines(lines, i + 1)
                
                if title:
                    if not isinstance(artist, str):
                        artist = str(artist) if artist else ""
                    
                    artists = self.parse_multiple_artists(artist)
                    songs.append({
                        'position': tw_position,
                        'title': title,
                        'artist': artists
                    })
                i += 1
                continue
            
            i += 1
        
        return songs
    
    def find_title_in_next_lines(self, lines, start_index):
        for j in range(start_index, min(start_index + 3, len(lines))):
            if j >= len(lines):
                break
            
            line = lines[j].strip()
            
            if (not line or 
                any(skip in line.lower() for skip in ['download', 'amazon', 'img', 'src=', 'http', '![]']) or
                re.match(r'^[\|\s\-]*$', line) or
                re.match(r'^\d+\s*\|\s*\d+', line) or
                line.isdigit()):
                continue
            
            clean_line = re.sub(r'^[\|\s]+|[\|\s]+$', '', line)
            clean_line = re.sub(r'\s+', ' ', clean_line).strip()
            
            if clean_line and len(clean_line) >= 2:
                return clean_line
        
        return ""
    
    def find_artist_in_text_lines(self, lines, start_index):
        for j in range(start_index, min(start_index + 12, len(lines))):
            if j >= len(lines):
                break
            
            line = lines[j].strip()
            
            if (not line or 
                any(skip in line.lower() for skip in ['download', 'amazon', 'img', 'src=', 'http', '![]']) or
                re.match(r'^[\|\s\-]*$', line) or
                re.match(r'^\[TW\]peaks', line) or
                re.match(r'^(\d+|\-)\s*\|\s*(\d+)', line) or
                line.isdigit() or
                len(line) > 150 or
                line.startswith('[') or
                '../../' in line or
                'week' in line.lower() or
                'chart' in line.lower()):
                continue
            
            clean_line = re.sub(r'^[\|\s]+|[\|\s]+$', '', line)
            clean_line = re.sub(r'\s+', ' ', clean_line).strip()
            
            if (clean_line and 
                len(clean_line) >= 2 and 
                len(clean_line) < 150 and
                any(c.isalpha() for c in clean_line) and
                not clean_line.isdigit()):
                
                if (not any(word in clean_line.lower() for word in ['peak', 'week', 'chart', 'html']) and
                    not re.match(r'^\d+[\s\|]', clean_line)):
                    return clean_line
        
        return ""
    
    def extract_chart_date_from_page(self, soup):
        try:
            month_names = {
                'january': 1, 'jan': 1,
                'february': 2, 'feb': 2,
                'march': 3, 'mar': 3,
                'april': 4, 'apr': 4,
                'may': 5,
                'june': 6, 'jun': 6,
                'july': 7, 'jul': 7,
                'august': 8, 'aug': 8,
                'september': 9, 'sep': 9, 'sept': 9,
                'october': 10, 'oct': 10,
                'november': 11, 'nov': 11,
                'december': 12, 'dec': 12
            }
            
            headings = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
            
            for heading in headings:
                heading_text = heading.get_text().strip()
                
                date_patterns = [
                    r'for\s+([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})',
                    r'([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})',
                    r'(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})',
                    r'([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})'
                ]
                
                for pattern in date_patterns:
                    date_match = re.search(pattern, heading_text, re.IGNORECASE)
                    if date_match:
                        groups = date_match.groups()
                        
                        try:
                            if len(groups) == 3 and groups[0].lower() in month_names:
                                month_str = groups[0].lower()
                                day = int(groups[1])
                                year = int(groups[2])
                                month = month_names[month_str]
                                
                                from datetime import datetime
                                parsed_date = datetime(year, month, day)
                                return parsed_date.strftime('%Y-%m-%d')
                            
                            elif len(groups) == 3 and groups[1].lower() in month_names:
                                day = int(groups[0])
                                month_str = groups[1].lower()
                                year = int(groups[2])
                                month = month_names[month_str]
                                
                                from datetime import datetime
                                parsed_date = datetime(year, month, day)
                                return parsed_date.strftime('%Y-%m-%d')
                                
                        except (ValueError, KeyError):
                            continue
            
            all_text_elements = soup.find_all(text=True)
            for text in all_text_elements:
                if isinstance(text, str) and len(text.strip()) > 10:
                    text = text.strip()
                    
                    date_patterns = [
                        r'for\s+([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})',
                        r'([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})',
                        r'(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})',
                        r'([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})'
                    ]
                    
                    for pattern in date_patterns:
                        date_match = re.search(pattern, text, re.IGNORECASE)
                        if date_match:
                            groups = date_match.groups()
                            
                            try:
                                if len(groups) == 3 and groups[0].lower() in month_names:
                                    month_str = groups[0].lower()
                                    day = int(groups[1])
                                    year = int(groups[2])
                                    month = month_names[month_str]
                                    
                                    if 1900 <= year <= 2030 and 1 <= month <= 12 and 1 <= day <= 31:
                                        from datetime import datetime
                                        parsed_date = datetime(year, month, day)
                                        return parsed_date.strftime('%Y-%m-%d')
                                
                                elif len(groups) == 3 and groups[1].lower() in month_names:
                                    day = int(groups[0])
                                    month_str = groups[1].lower()
                                    year = int(groups[2])
                                    month = month_names[month_str]
                                    
                                    if 1900 <= year <= 2030 and 1 <= month <= 12 and 1 <= day <= 31:
                                        from datetime import datetime
                                        parsed_date = datetime(year, month, day)
                                        return parsed_date.strftime('%Y-%m-%d')
                                        
                            except (ValueError, KeyError):
                                continue
            
            table_cells = soup.find_all(['td', 'th'])
            for cell in table_cells:
                cell_text = cell.get_text().strip()
                if len(cell_text) > 10 and any(month in cell_text.lower() for month in month_names.keys()):
                    date_patterns = [
                        r'([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})',
                        r'(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})'
                    ]
                    
                    for pattern in date_patterns:
                        date_match = re.search(pattern, cell_text, re.IGNORECASE)
                        if date_match:
                            groups = date_match.groups()
                            
                            try:
                                if len(groups) == 3 and groups[0].lower() in month_names:
                                    month_str = groups[0].lower()
                                    day = int(groups[1])
                                    year = int(groups[2])
                                    month = month_names[month_str]
                                    
                                    if 1900 <= year <= 2030 and 1 <= month <= 12 and 1 <= day <= 31:
                                        from datetime import datetime
                                        parsed_date = datetime(year, month, day)
                                        return parsed_date.strftime('%Y-%m-%d')
                                
                                elif len(groups) == 3 and groups[1].lower() in month_names:
                                    day = int(groups[0])
                                    month_str = groups[1].lower()
                                    year = int(groups[2])
                                    month = month_names[month_str]
                                    
                                    if 1900 <= year <= 2030 and 1 <= month <= 12 and 1 <= day <= 31:
                                        from datetime import datetime
                                        parsed_date = datetime(year, month, day)
                                        return parsed_date.strftime('%Y-%m-%d')
                                        
                            except (ValueError, KeyError):
                                continue
            
        except Exception as e:
            print(f"Error extracting date from page: {e}")
        
        return None
    
    def extract_chart_date_from_url(self, url):
        match = re.search(r'/charts/(\d{2})/(?:rock|week)(\d{4})\.html', url)
        if match:
            decade = int(match.group(1))
            week_number = match.group(2)
            year_suffix = int(week_number[:2])
            week = int(week_number[2:])
            
            if decade >= 60 and decade <= 99:
                full_year = 1900 + year_suffix
            elif decade >= 0 and decade <= 20:
                full_year = 2000 + year_suffix
            else:
                full_year = 1900 + year_suffix if year_suffix >= 60 else 2000 + year_suffix
            
            try:
                import datetime
                jan_1 = datetime.date(full_year, 1, 1)
                days_to_monday = (7 - jan_1.weekday()) % 7
                first_monday = jan_1 + datetime.timedelta(days=days_to_monday)
                week_start = first_monday + datetime.timedelta(weeks=week-1)
                return week_start.strftime('%Y-%m-%d')
            except (ValueError, OverflowError):
                return f"{full_year}-01-01"
        
        print(f"Error: Could not extract date from URL: {url}")
        return None
    
    def generate_record_id(self, url, position):
        url_match = re.search(r'(?:rock|week)(\d{4})\.html', url)
        chart_id = url_match.group(1) if url_match else "0000"
        chart_type = "rock" if "rock" in url else "pop"
        return f"{chart_type}_{chart_id}_{position:03d}"
        
    def extract_title_from_cell(self, title_cell):
        link = title_cell.find('a', class_='songLink')
        if link:
            return link.get_text().strip()
        
        title = title_cell.get_text().strip()
        return re.sub(r'\s+', ' ', title).strip() if title else ""
    
    def find_artist_in_next_tables(self, tables, start_index):
        for j in range(start_index, min(start_index + 20, len(tables))):
            if j >= len(tables):
                break
            
            table = tables[j]
            artist_cell = table.find('td', class_='artist20')
            
            if artist_cell:
                artist_links = artist_cell.find_all('a', class_='artistLink')
                if len(artist_links) > 1:
                    artist_names = []
                    for link in artist_links:
                        name = link.get_text().strip()
                        if name and len(name) > 1:
                            artist_names.append(name)
                    
                    if len(artist_names) > 1:
                        return ' with '.join(artist_names)
                    elif len(artist_names) == 1:
                        return artist_names[0]
                
                artist = self.extract_artist_from_cell(artist_cell)
                if artist:
                    return artist
            
            all_cells = table.find_all('td')
            for cell in all_cells:
                cell_text = cell.get_text().strip()
                
                if not cell_text or len(cell_text) < 2:
                    continue
                
                if (cell_text.isdigit() or 
                    cell_text in ['-', '|'] or
                    any(skip in cell_text.lower() for skip in ['download', 'youtube', 'amazon', 'http', '../../', 'week', 'chart', 'peak', 'html', 'img']) or
                    cell_text.startswith('[') or
                    re.match(r'^\d+\s*\|\s*\d+', cell_text) or
                    cell_text == '![]()'):
                    continue
                
                if (len(cell_text) > 1 and len(cell_text) < 200 and
                    any(c.isalpha() for c in cell_text)):
                    return cell_text
        
        return ""
    
    def extract_artist_from_cell(self, artist_cell):
        artists = []
        
        artist_links = artist_cell.find_all('a', class_='artistLink')
        
        if artist_links:
            for link in artist_links:
                artist_name = link.get_text().strip()
                if artist_name and len(artist_name) > 1:
                    artists.append(artist_name)
            
            if len(artists) > 1:
                return ' with '.join(artists)
            elif len(artists) == 1:
                return artists[0]
        
        artist_text = artist_cell.get_text().strip()
        return re.sub(r'\s+', ' ', artist_text).strip() if artist_text else ""
    
    def parse_multiple_artists(self, artist_text):
        if not artist_text:
            return []
        
        if isinstance(artist_text, list):
            return artist_text
        
        if not isinstance(artist_text, str):
            artist_text = str(artist_text)
        
        artist_text = artist_text.strip()
        if not artist_text:
            return []
        
        separators = [
            r'\s+with\s+',
            r'\s+featuring\s+',
            r'\s+feat\.?\s+',
            r'\s+ft\.?\s+', 
            r'\s+and\s+',
            r'\s+&\s+',
            r'\s*,\s+(?=\w)',
        ]
        
        for separator in separators:
            parts = re.split(separator, artist_text, flags=re.IGNORECASE)
            if len(parts) > 1:
                cleaned_artists = []
                for part in parts:
                    cleaned_part = part.strip()
                    cleaned_part = re.sub(r'^(the\s+|a\s+)', '', cleaned_part, flags=re.IGNORECASE)
                    cleaned_part = cleaned_part.strip()
                    
                    if cleaned_part and len(cleaned_part) > 1:
                        cleaned_artists.append(cleaned_part)
                
                if len(cleaned_artists) > 1:
                    return cleaned_artists
        
        return [artist_text]
    
    def clean_songs(self, songs):
        seen_positions = set()
        unique_songs = []
        
        for song in songs:
            if not song or not song.get('position'):
                continue
            
            position = song['position']
            if position in seen_positions:
                continue
            
            title = song.get('title', '').strip()
            artist_data = song.get('artist', '')
            
            if not title:
                continue
            
            if isinstance(artist_data, list):
                artists = artist_data
            elif isinstance(artist_data, str):
                artists = self.parse_multiple_artists(artist_data)
            else:
                artists = []
            
            unique_songs.append({
                'position': position,
                'title': title,
                'artist': artists
            })
            
            seen_positions.add(position)
        
        return unique_songs
    
    async def scrape_all_charts_sequential(self):
        print("\nStarting sequential chart scraping...")
        print("Processing Order: ALL ROCK CHARTS FIRST, THEN ALL POP CHARTS")
        print("Priority: 2010 charts will be processed first within each category!")
        
        self.load_progress()
        
        total_charts = len(self.pop_urls) + len(self.rock_urls)
        current_chart = 0
        
        # PHASE 1: Process ALL Rock Charts FIRST
        print(f"\nPHASE 1: SCRAPING ALL {len(self.rock_urls)} ROCK CHARTS")
        for i, url in enumerate(self.rock_urls, 1):
            current_chart += 1
            
            if url in self.processed_urls:
                print(f"[{current_chart}/{total_charts}] {i}/{len(self.rock_urls)} - SKIPPED (Rock)")
                continue
                
            # Show if it's a 2010 chart
            is_2010 = '/charts/10/' in url
            year_indicator = " [2010]" if is_2010 else ""
            print(f"[{current_chart}/{total_charts}] {i}/{len(self.rock_urls)} - {url} (Rock){year_indicator}")
            
            try:
                chart_data = await self.scrape_single_chart(url, 'rock')
                
                if chart_data:
                    records_count = len(chart_data['records'])
                    self.all_chart_data.append(chart_data)
                    
                    chart_date = chart_data['chart_info']['chart_date']
                    chart_type = chart_data['chart_info']['chart_type'].upper()
                    print(f"Chart Date: {chart_date} | Type: {chart_type}")
                    
                    for record in chart_data['records'][:3]:
                        rank = record['rank']
                        title = record['title']
                        artists = json.loads(record['artist'])
                        if isinstance(artists, list) and artists:
                            artist_display = ', '.join(artists)
                        else:
                            artist_display = '[No Artist]'
                        print(f"   {rank}. {title} - {artist_display}")
                    
                    self.save_progress(url)
                    self.save_incremental_data()
                    print(f"Success: {records_count} records")
                    
                else:
                    print("Failed to scrape")
                    self.save_progress(url)
            
            except Exception as e:
                print(f"Error: {e}")
                self.save_progress(url)
            
            await asyncio.sleep(2)
        
        # PHASE 2: Process ALL Pop Charts AFTER Rock Charts
        print(f"\nPHASE 2: SCRAPING ALL {len(self.pop_urls)} POP CHARTS")
        for i, url in enumerate(self.pop_urls, 1):
            current_chart += 1
            
            if url in self.processed_urls:
                print(f"[{current_chart}/{total_charts}] {i}/{len(self.pop_urls)} - SKIPPED (Pop)")
                continue
                
            # Show if it's a 2010 chart
            is_2010 = '/charts/10/' in url
            year_indicator = " [2010]" if is_2010 else ""
            print(f"[{current_chart}/{total_charts}] {i}/{len(self.pop_urls)} - {url} (Pop){year_indicator}")
            
            try:
                chart_data = await self.scrape_single_chart(url, 'pop')
                
                if chart_data:
                    records_count = len(chart_data['records'])
                    self.all_chart_data.append(chart_data)
                    
                    chart_date = chart_data['chart_info']['chart_date']
                    chart_type = chart_data['chart_info']['chart_type'].upper()
                    print(f"Chart Date: {chart_date} | Type: {chart_type}")
                    
                    for record in chart_data['records'][:3]:
                        rank = record['rank']
                        title = record['title']
                        artists = json.loads(record['artist'])
                        if isinstance(artists, list) and artists:
                            artist_display = ', '.join(artists)
                        else:
                            artist_display = '[No Artist]'
                        print(f"   {rank}. {title} - {artist_display}")
                    
                    self.save_progress(url)
                    self.save_incremental_data()
                    print(f"Success: {records_count} records")
                    
                else:
                    print("Failed to scrape")
                    self.save_progress(url)
            
            except Exception as e:
                print(f"Error: {e}")
                self.save_progress(url)
            
            await asyncio.sleep(2)
    
    def save_incremental_data(self):
        try:
            os.makedirs('data', exist_ok=True)
            filename = 'data/charts_data.csv'
            file_exists = os.path.exists(filename)
            
            import csv
            mode = 'a' if file_exists else 'w'
            with open(filename, mode, encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(['chart_date', 'chart_type', 'rank', 'title', 'artist', 'url'])
                
                for chart in self.all_chart_data[-1:]:
                    chart_type = chart['chart_info']['chart_type']
                    for record in chart['records']:
                        artists = json.loads(record['artist']) if isinstance(record['artist'], str) else record['artist']
                        artist_str = ', '.join(artists) if artists else ''
                        writer.writerow([
                            record['chart_date'],
                            chart_type,
                            record['rank'],
                            record['title'],
                            artist_str,
                            record['url']
                        ])
            
            total_charts = len(self.all_chart_data)
            total_records = sum(len(chart.get('records', [])) for chart in self.all_chart_data)
            pop_charts = len([c for c in self.all_chart_data if c['chart_info']['chart_type'] == 'pop'])
            rock_charts = len([c for c in self.all_chart_data if c['chart_info']['chart_type'] == 'rock'])
            
            print(f"Saved: {total_charts} charts, {total_records} records (Rock: {rock_charts}, Pop: {pop_charts})")
            
        except Exception as e:
            print(f"Save failed: {e}")
    
    def print_final_summary(self):
        if not self.all_chart_data:
            print("No data to summarize")
            return
        
        pop_count = len([c for c in self.all_chart_data if c['chart_info']['chart_type'] == 'pop'])
        rock_count = len([c for c in self.all_chart_data if c['chart_info']['chart_type'] == 'rock'])
        total_records = sum(len(chart.get('records', [])) for chart in self.all_chart_data)
        
        # Count 2010 charts
        pop_2010_count = len([c for c in self.all_chart_data if c['chart_info']['chart_type'] == 'pop' and '/charts/10/' in c['chart_info']['url']])
        rock_2010_count = len([c for c in self.all_chart_data if c['chart_info']['chart_type'] == 'rock' and '/charts/10/' in c['chart_info']['url']])
        
        print("\nTUNECASTER SCRAPING COMPLETE")
        print("="*60)
        print(f"Rock Charts: {rock_count} (2010: {rock_2010_count})")
        print(f"Pop Charts: {pop_count} (2010: {pop_2010_count})")
        print(f"Total Charts: {len(self.all_chart_data)}")
        print(f"Total 2010 Charts: {pop_2010_count + rock_2010_count}")
        print(f"Total Records: {total_records}")
        print(f"Data File: data/charts_data.csv")
        print(f"Progress File: data/scraper_progress.json")
        print("="*60)
        print("PROCESSING ORDER WAS: ALL ROCK FIRST, THEN ALL POP")
        print("="*60)

async def main():
    scraper = TuneCasterCompleteScraper()
    
    print("TuneCaster Complete Scraper")
    print("ROCK FIRST, THEN POP (2010 Priority)")
    print("="*60)
    
    try:
        await scraper.discover_all_chart_urls()
        
        if not scraper.pop_urls and not scraper.rock_urls:
            print("No chart URLs found. Exiting.")
            return
        
        await scraper.scrape_all_charts_sequential()
        scraper.print_final_summary()
        
        print("Scraping completed!")
        
    except KeyboardInterrupt:
        print("\nScraping interrupted")
        if scraper.all_chart_data:
            scraper.print_final_summary()
        print("Run script again to resume")
    
    except Exception as e:
        print(f"Error: {e}")
        if scraper.all_chart_data:
            scraper.print_final_summary()

if __name__ == "__main__":
    asyncio.run(main())