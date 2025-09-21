#!/usr/bin/env python3
"""
Unified Anime Bot - Render Compatible
Combines auto caption formatting, file sequencing, and bulk upload parsing
Optimized for Render deployment with PostgreSQL
"""

import os
import re
import json
import asyncio
import asyncpg
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlparse
import urllib.parsefrom telegram import Update
from telegram import ContextTypes
    

from telegram import Update, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters
)
from telegram.error import TelegramError

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class VideoFile:
    """Video file class for sequencing functionality"""
    def __init__(self, file_id: str, filename: str, caption: Optional[str] = None, file_type: str = 'document'):
        self.file_id = file_id
        self.filename = filename
        self.caption = caption or ''
        self.file_type = file_type
        self.episode_number = self.extract_episode_number()
        self.video_quality = self.extract_video_quality()

    def extract_episode_number(self) -> Optional[int]:
        """Extract episode number from filename or caption"""
        pattern = r'\[S\d+-E(\d+)\]'
        for text in [self.filename, self.caption]:
            match = re.search(pattern, text)
            if match:
                return int(match.group(1))
        return None

    def extract_video_quality(self) -> Optional[int]:
        """Extract video quality from filename or caption"""
        pattern = r'\[S\d+-E\d+\].*\[(\d+)(P)?\]'
        for text in [self.filename, self.caption]:
            match = re.search(pattern, text)
            if match:
                quality = int(match.group(1))
                common_qualities = [144, 240, 360, 480, 720, 1080, 1440, 2160]
                return quality if quality in common_qualities else None
        return None

class AnimeParser:
    """Enhanced anime caption parser"""
    def __init__(self):
        self.patterns = {
            'bracket_se': r'\[S(\d+)\s*E(\d+)\]',
            'bracket_sep': r'\[S(\d+)\s*EP(\d+)\]',
            'channel_se': r'@\w+\s*-\s*(.+?)\s+S(\d+)\s*EP(\d+)',
            'channel_bracket': r'@\w+\s*-\s*\[S(\d+)\s*EP(\d+)\]\s*(.+?)(?:\s*\[|$)',
            'structured_emoji': r'📺\s*([^\[]+)\s*\[S(\d+)\]',
            'simple_se': r'S(\d+)\s*E(\d+)',
            'simple_ep': r'S(\d+)\s*EP(\d+)',
        }
    
    def extract_episode_info(self, text):
        """Extract season, episode, and anime name"""
        season = "01"
        episode = "01"
        anime_name = ""
        
        clean_text = text.strip()
        
        if "📺" in clean_text and "Eᴘɪꜱᴏᴅᴇ" in clean_text:
            return self._parse_structured_format(clean_text)
        
        for pattern_name in ['channel_se', 'channel_bracket']:
            pattern = self.patterns[pattern_name]
            match = re.search(pattern, clean_text, re.IGNORECASE)
            if match:
                if pattern_name == 'channel_se':
                    anime_name, season, episode = match.groups()
                else:
                    season, episode, anime_name = match.groups()
                return season.zfill(2), episode.zfill(2), anime_name.strip()
        
        for pattern_name in ['bracket_se', 'bracket_sep']:
            pattern = self.patterns[pattern_name]
            match = re.search(pattern, clean_text, re.IGNORECASE)
            if match:
                season, episode = match.groups()
                anime_name = re.split(r'\[S\d+', clean_text, flags=re.IGNORECASE)[0].strip()
                return season.zfill(2), episode.zfill(2), anime_name
        
        for pattern_name in ['simple_se', 'simple_ep']:
            pattern = self.patterns[pattern_name]
            match = re.search(pattern, clean_text, re.IGNORECASE)
            if match:
                season, episode = match.groups()
                anime_name = re.split(r'S\d+', clean_text, flags=re.IGNORECASE)[0].strip()
                return season.zfill(2), episode.zfill(2), anime_name
        
        return season, episode, clean_text
    
    def _parse_structured_format(self, text):
        """Parse structured format with emojis"""
        season = "01"
        episode = "01"
        anime_name = ""
        
        title_match = re.search(r'📺\s*([^\[]+)\s*\[S(\d+)\]', text, re.IGNORECASE)
        if title_match:
            anime_name = title_match.group(1).strip()
            season = title_match.group(2).zfill(2)
        
        episode_match = re.search(r'Eᴘɪꜱᴏᴅᴇ\s*:\s*(\d+)', text, re.IGNORECASE)
        if episode_match:
            episode = episode_match.group(1).zfill(2)
        
        return season, episode, anime_name
    
    def extract_quality(self, text):
        """Extract video quality and ensure it ends with 'P'"""
        quality_patterns = [
            r'(\d+)[pP]',
            r'\[(\d+)[pP]?\]',
            r'Qᴜᴀʟɪᴛʏ\s*:\s*(\d+)[pP]?',
            r'QUALITY\s*:\s*(\d+)[pP]?',
            r'(\d+)\s*[pP]',
        ]
        
        for pattern in quality_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                quality_number = match.group(1)
                if int(quality_number) in [144, 240, 360, 480, 720, 1080, 1440, 2160]:
                    return f"{quality_number}P"
        
        return "720P"
    
    def extract_language(self, text):
        """Extract language/audio information"""
        language_mappings = {
            'தமிழ்': 'Tam', 'tamil': 'Tam', 'tam': 'Tam',
            'english': 'Eng', 'eng': 'Eng',
            'multi audio': 'Multi', 'multi': 'Multi',
            'dual audio': 'Dual', 'dual': 'Dual',
        }
        
        audio_match = re.search(r'(?:Aᴜᴅɪᴏ|Audio)\s*:\s*([^,\n\]]+)', text, re.IGNORECASE)
        if audio_match:
            audio_text = audio_match.group(1).strip().lower()
            for key, value in language_mappings.items():
                if key in audio_text:
                    return value
        
        text_lower = text.lower()
        for key, value in language_mappings.items():
            if key in text_lower:
                return value
        
        return ""
    
    def clean_anime_name(self, name):
        """Clean and standardize anime name"""
        if not name:
            return ""
        
        name = re.sub(r'^@\w+\s*-\s*', '', name, flags=re.IGNORECASE)
        
        unwanted_patterns = [
            r'\[.*?\]', r'\(.*?\)',
            r'^\s*-\s*', r'\s*-\s*$',
        ]
        
        for pattern in unwanted_patterns:
            name = re.sub(pattern, '', name, flags=re.IGNORECASE)
        
        replacements = {
            'Tamil': 'Tam', 'English': 'Eng',
            'Dubbed': 'Dub', 'Subbed': 'Sub',
        }
        
        for old, new in replacements.items():
            name = re.sub(rf'\b{old}\b', new, name, flags=re.IGNORECASE)
        
        name = re.sub(r'[!@#$%^&*(),.?":{}|<>]', '', name)
        name = re.sub(r'\s+', ' ', name).strip()
        
        return name

class UnifiedAnimeBot:
    """Unified bot combining all functionalities"""
    
    def __init__(self):
        self.db_pool = None
        self.user_sessions: Dict[int, List[VideoFile]] = {}
        self.dump_channels: Dict[int, str] = {}
        self.fixed_anime_name = ""
        self.message_count = 0
        self.prefixes = ["/leech -n", "/leech1 -n", "/leech2 -n", "/leechx -n", "/leech3 -n", "/leech5 -n"]
        self.log_channel_id = os.getenv("LOG_CHANNEL_ID", "")
        self.authorized_admins = set()
        self.target_channel = None
        self.parser = AnimeParser()
        
        # Load admin IDs from environment
        admin_ids = os.getenv("ADMIN_IDS", "")
        if admin_ids.strip():
            for admin_id in admin_ids.split(","):
                if admin_id.strip().isdigit():
                    self.authorized_admins.add(int(admin_id.strip()))

    async def init_database(self):
        """Initialize PostgreSQL database with all required tables"""
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            logger.error("DATABASE_URL environment variable not set")
            return False
        
        try:
            logger.info("Connecting to database...")
            self.db_pool = await asyncpg.create_pool(
                database_url,
                min_size=1,
                max_size=5,
                command_timeout=60
            )
            
            async with self.db_pool.acquire() as conn:
                # Anime table
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS anime (
                        anime_id SERIAL PRIMARY KEY,
                        anime_name VARCHAR(200) UNIQUE NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Episodes table
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS episodes (
                        id SERIAL PRIMARY KEY,
                        anime_id INTEGER REFERENCES anime(anime_id) ON DELETE CASCADE,
                        episode VARCHAR(50) NOT NULL,
                        quality VARCHAR(20) NOT NULL,
                        file_name VARCHAR(300) NOT NULL,
                        file_type VARCHAR(50) DEFAULT 'Single',
                        url TEXT NOT NULL,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(anime_id, episode, quality, url)
                    )
                """)
                
                # Bot configuration table
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS bot_config (
                        key VARCHAR(50) PRIMARY KEY,
                        value TEXT NOT NULL,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # User settings table
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS user_settings (
                        user_id BIGINT PRIMARY KEY,
                        dump_channel TEXT,
                        fixed_anime_name TEXT,
                        prefixes JSONB DEFAULT '[]',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Create indexes
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_anime_name ON anime(anime_name)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_episodes_anime_id ON episodes(anime_id)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_episodes_episode ON episodes(episode)")
                
            logger.info("Database initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            return False

    async def load_user_settings(self, user_id: int):
        """Load user-specific settings from database"""
        if not self.db_pool:
            return
            
        try:
            async with self.db_pool.acquire() as conn:
                settings = await conn.fetchrow(
                    "SELECT * FROM user_settings WHERE user_id = $1", user_id
                )
                
                if settings:
                    if settings['dump_channel']:
                        self.dump_channels[user_id] = settings['dump_channel']
                    if settings['fixed_anime_name']:
                        self.fixed_anime_name = settings['fixed_anime_name']
                    if settings['prefixes']:
                        self.prefixes = json.loads(settings['prefixes'])
                        
        except Exception as e:
            logger.error(f"Error loading user settings: {e}")

    async def save_user_settings(self, user_id: int):
        """Save user-specific settings to database"""
        if not self.db_pool:
            return
    
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO user_settings (user_id, dump_channel, fixed_anime_name, prefixes)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (user_id)
                    DO UPDATE SET
                        dump_channel = $2,
                        fixed_anime_name = $3,
                        prefixes = $4
                """, user_id,
                    self.dump_channels.get(user_id, ""),
                    self.fixed_anime_name,
                    json.dumps(self.prefixes)
                )
        except Exception as e:
            logger.error(f"Error saving user settings: {e}")

    async def log_action(self, context: ContextTypes.DEFAULT_TYPE, user_id: int, username: str, action: str, details: str = ""):
        """Log user actions to designated log channel"""
        if not self.log_channel_id:
            return
            
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
        log_message = (
            f"🕒 {timestamp}\n"
            f"👤 User: {username} (ID: {user_id})\n"
            f"📋 Action: {action}\n"
            f"📝 Details: {details}"
        )
        
        try:
            await context.bot.send_message(
                chat_id=self.log_channel_id,
                text=log_message
            )
        except Exception as e:
            logger.error(f"Failed to log action: {e}")

    def parse_caption(self, caption: str, user_id: int) -> str:
        """Enhanced caption parser with support for multiple formats"""
        self.message_count += 1
        
        if not caption:
            return ""
        
        original_caption = caption.strip()
        clean_caption = original_caption
        
        if " - " in clean_caption and clean_caption.startswith("@"):
            parts = clean_caption.split(" - ", 1)
            if len(parts) > 1:
                clean_caption = parts[1]
        
        season, episode, extracted_name = self.parser.extract_episode_info(original_caption)
        quality = self.parser.extract_quality(original_caption)
        language = self.parser.extract_language(original_caption)
        
        if self.fixed_anime_name:
            anime_name = self.fixed_anime_name
        else:
            anime_name = self.parser.clean_anime_name(extracted_name) or "Unknown Anime"
        
        if language and language not in anime_name:
            anime_name = f"{anime_name} {language}".strip()
        
        season_episode = f"[S{season}-E{episode}]"
        
        extension = ".mkv"
        if ".mp4" in original_caption.lower():
            extension = ".mp4"
        elif ".avi" in original_caption.lower():
            extension = ".avi"
        
        if self.prefixes:
            prefix_index = (self.message_count - 1) // 3 % len(self.prefixes)
            current_prefix = self.prefixes[prefix_index]
        else:
            current_prefix = "/leech -n"
        
        formatted_caption = f"{current_prefix} {season_episode} {anime_name} [{quality}] [Single]{extension}"
        
        return formatted_caption

    def parse_bulk_message(self, text: str) -> List[Dict]:
        """Parse bulk upload message and extract structured data"""
        results = []
        lines = text.strip().split('\n')
        
        for line in lines:
            line = line.strip()
            if not line or line.startswith('📦'):
                continue
                
            pattern = r'(\d+)\.\s*(.+?)\s*-\s*(https?://[^\s]+)'
            match = re.match(pattern, line)
            
            if not match:
                continue
                
            entry_num = match.group(1)
            content_part = match.group(2).strip()
            url = match.group(3)
            
            brackets = re.findall(r'\[([^\]]+)\]', content_part)
            
            episode = f"EP{entry_num}"
            quality = "720p"
            file_type = "Single"
            
            for bracket in brackets:
                if re.match(r'S\d+[-E]\d+|S\d+E\d+|EP?\d+', bracket, re.IGNORECASE):
                    episode = bracket
                elif re.match(r'^\d{3,4}p?$', bracket):
                    quality = bracket if bracket.endswith('p') else bracket + 'p'
                elif bracket.lower() in ['single', 'batch', 'dual', 'multi']:
                    file_type = bracket.capitalize()
            
            anime_name = re.sub(r'\[([^\]]+)\]', '', content_part).strip()
            anime_name = re.sub(r'\.(mkv|mp4|avi)$', '', anime_name, re.IGNORECASE)
            anime_name = re.sub(r'\s+', ' ', anime_name).strip()
            
            if not anime_name:
                anime_name = f"Unknown Anime {entry_num}"
            
            file_name = content_part.strip()
            
            results.append({
                'anime_name': anime_name,
                'episode': episode,
                'quality': quality,
                'file_name': file_name,
                'file_type': file_type,
                'url': url
            })
        
        return results

    async def get_or_create_anime(self, anime_name: str) -> int:
        """Get existing anime_id or create new anime entry"""
        async with self.db_pool.acquire() as conn:
            anime = await conn.fetchrow(
                "SELECT anime_id FROM anime WHERE anime_name = $1", anime_name
            )
            
            if anime:
                return anime['anime_id']
            
            anime_id = await conn.fetchval(
                "INSERT INTO anime (anime_name) VALUES ($1) RETURNING anime_id",
                anime_name
            )
            return anime_id

    async def store_episodes(self, entries: List[Dict]) -> Tuple[int, int, Dict]:
        """Store episodes in database"""
        stored = 0
        duplicates = 0
        anime_summary = {}
        
        if not self.db_pool:
            return stored, duplicates, anime_summary
            
        for entry in entries:
            try:
                anime_id = await self.get_or_create_anime(entry['anime_name'])
                
                if anime_id not in anime_summary:
                    anime_summary[anime_id] = {
                        'name': entry['anime_name'],
                        'episodes': []
                    }
                
                async with self.db_pool.acquire() as conn:
                    try:
                        episode_id = await conn.fetchval("""
                            INSERT INTO episodes (anime_id, episode, quality, file_name, file_type, url)
                            VALUES ($1, $2, $3, $4, $5, $6)
                            RETURNING id
                        """, anime_id, entry['episode'], entry['quality'],
                             entry['file_name'], entry['file_type'], entry['url'])
                        
                        stored += 1
                        anime_summary[anime_id]['episodes'].append({
                            'id': episode_id,
                            'episode': entry['episode'],
                            'quality': entry['quality']
                        })
                        
                    except asyncpg.UniqueViolationError:
                        duplicates += 1
                        
            except Exception as e:
                logger.error(f"Error storing episode: {e}")
                duplicates += 1
        
        return stored, duplicates, anime_summary

    async def send_to_dump_channel(self, context: ContextTypes.DEFAULT_TYPE, user_id: int, message, formatted_caption: str):
        """Send formatted caption to dump channel"""
        dump_channel = self.dump_channels.get(user_id)
        if not dump_channel:
            return False, "Dump channel not configured"
        
        try:
            await context.bot.send_message(
                chat_id=dump_channel,
                text=f"📤 **Auto-formatted Caption**\n\n`{formatted_caption}`\n\n⏰ Processed at: {message.date}",
                parse_mode='Markdown'
            )
            return True, "Success"
        except Exception as e:
            logger.error(f"Failed to send to dump channel: {e}")
            return False, str(e)

    # COMMAND HANDLERS

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        user_id = update.effective_user.id
        username = update.effective_user.username or "Unknown"
        
        await self.load_user_settings(user_id)
        
        welcome_message = f"""
🤖 **Unified Anime Bot - All-in-One Solution**
🖥️ Deployed on Render Platform

**👋 Welcome {username}!**

**🎬 CAPTION FORMATTING:**
• Professional quality formatting (480P, 720P, 1080P)
• Dynamic prefix management  
• Multiple input format support
• Language detection (Tamil, English, Multi)

**📁 FILE SEQUENCING:**
• Sort video files by episode and quality
• Support for [S01-E07] format
• Quality-based organization (480p → 720p → 1080p)

**📚 BULK UPLOAD MANAGEMENT:**
• Parse and organize bulk upload messages
• Database storage with anime IDs
• Episode tracking and management
• Export functionality

        """
        
        await update.message.reply_text(welcome_message, parse_mode='Markdown')
        await self.log_action(context, user_id, username, "Started unified bot")

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show comprehensive help"""
        help_text = """
❓ **UNIFIED ANIME BOT - COMPLETE GUIDE**

**🎨 CAPTION FORMATTING COMMANDS:**
• `/name <anime>` - Set fixed anime name
• `/name reset` - Enable auto-detection
• `/format <text>` - Test caption formatting
• `/addprefix <prefix>` - Add new prefix
• `/prefixlist` - Show all prefixes
• `/delprefix <index>` - Delete prefix
• `/dumpchannel <id>` - Set dump channel
• `/quality` - Show quality formats

**📁 FILE SEQUENCING COMMANDS:**
• `/sequence` - Start collecting files
• `/endsequence` - Sort and send files
• `/dump <channel>` - Set sequence dump channel

**📚 BULK UPLOAD COMMANDS:**
• `/anime_list` - Show all anime with IDs
• `/list <anime_id>` - List episodes for anime
• `/search <anime_id> <episode>` - Find episode
• `/delete <anime_id> <ep_id>` - Delete episode
• `/clear <anime_id>` - Clear all episodes
• `/export_excel` - Export to Excel
• `/stats` - Database statistics

**⚙️ ADMIN COMMANDS:**
• `/clear_db` - Clear entire database
• `/set_channel <@channel>` - Set target channel
• `/search <id S Q>
• `/status` - Show bot status

**📝 SUPPORTED FORMATS:**
• Caption: `[S01 E12] Anime [1080p] Tamil.mkv`
• Bulk: `1. [S01-E01] Anime Name [720p] - https://url`
• Sequence: Files with `[S01-E07]` format

Just send your content and let the bot handle the rest! 🚀
        """
        
        await update.message.reply_text(help_text, parse_mode='Markdown')

    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show comprehensive bot status"""
        user_id = update.effective_user.id
        
        # Get database stats
        anime_count = 0
        episode_count = 0
        
        if self.db_pool:
            try:
                async with self.db_pool.acquire() as conn:
                    anime_count = await conn.fetchval("SELECT COUNT(*) FROM anime")
                    episode_count = await conn.fetchval("SELECT COUNT(*) FROM episodes")
            except Exception as e:
                logger.error(f"Error getting stats: {e}")
        
        current_name = self.fixed_anime_name or "Auto-detect mode"
        current_prefix = self.prefixes[(self.message_count // 3) % len(self.prefixes)] if self.prefixes else "No prefixes"
        dump_status = "✅ Configured" if self.dump_channels.get(user_id) else "❌ Not set"
        session_status = "✅ Active" if user_id in self.user_sessions else "❌ Inactive"
        
        status_message = f"""
📊 **UNIFIED BOT STATUS**

**🎬 CAPTION FORMATTING:**
• Anime Name: {current_name}
• Messages Processed: {self.message_count}
• Current Prefix: `{current_prefix}`
• Total Prefixes: {len(self.prefixes)}
• Dump Channel: {dump_status}

**📁 FILE SEQUENCING:**
• Active Session: {session_status}
• Files in Queue: {len(self.user_sessions.get(user_id, []))}

**📚 DATABASE STORAGE:**
• Total Anime: {anime_count}
• Total Episodes: {episode_count}
• Database: {"✅ Connected" if self.db_pool else "❌ Disconnected"}

**⚙️ SYSTEM STATUS:**
• Platform: Render Cloud
• Admin Access: {"✅ Yes" if user_id in self.authorized_admins else "❌ No"}
• Log Channel: {"✅ Active" if self.log_channel_id else "❌ Not set"}

**🔄 ACTIVE FEATURES:**
• Multi-format Caption Parsing: ✅
• Quality Standardization: ✅
• Prefix Rotation: ✅
• File Sequencing: ✅
• Bulk Upload Processing: ✅
• Database Storage: ✅
• Export Functions: ✅

Use `/help` for complete command guide!
        """
        
        await update.message.reply_text(status_message, parse_mode='Markdown')

    async def name_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle anime name setting"""
        user_id = update.effective_user.id
        
        if not context.args:
            current_name = self.fixed_anime_name or "Auto-detect mode"
            await update.message.reply_text(
                f"📝 **Current anime name:** {current_name}\n\n"
                "**Usage:**\n"
                "• `/name YOUR ANIME NAME` - Set fixed name\n"
                "• `/name reset` - Enable auto-detection\n\n"
                "**Examples:**\n"
                "• `/name Naruto Shippuden Tam`\n"
                "• `/name One Piece Eng`\n"
                "• `/name reset`",
                parse_mode='Markdown'
            )
            return
        
        new_name = ' '.join(context.args).strip()
        
        if new_name.lower() == "reset":
            self.fixed_anime_name = ""
            await self.save_user_settings(user_id)
            await update.message.reply_text(
                "✅ **Fixed anime name reset!**\n\n"
                "Now using auto-detection mode.",
                parse_mode='Markdown'
            )
        else:
            self.fixed_anime_name = new_name
            await self.save_user_settings(user_id)
            await update.message.reply_text(
                f"✅ **Fixed anime name set!**\n\n"
                f"**Name:** {self.fixed_anime_name}\n\n"
                "All episodes will use this name until reset.",
                parse_mode='Markdown'
            )

    async def sequence_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start file sequence collection"""
        user_id = update.effective_user.id
        username = update.effective_user.username or "Unknown"
        
        self.user_sessions[user_id] = []
        
        message = (
            "📁 **Ready to receive files!** 📁\n\n"
            "Please start sending video files. Use `/endsequence` when done.\n\n"
            "**Expected format:** `[S01-E07] Show Name [1080P] [Single].mkv`"
        )
        
        await update.message.reply_text(message, parse_mode='Markdown')
        await self.log_action(context, user_id, username, "Started sequence collection")

    async def endsequence_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """End sequence and sort files"""
        user_id = update.effective_user.id
        username = update.effective_user.username or "Unknown"
        
        if user_id not in self.user_sessions or not self.user_sessions[user_id]:
            await update.message.reply_text(
                "❌ No files received! Use `/sequence` first and send video files."
            )
            return
        
        files = self.user_sessions[user_id]
        file_count = len(files)
        
        await update.message.reply_text(
            f"📊 Processing {file_count} files for sorting..."
        )
        
        await self.log_action(context, user_id, username, "Ended sequence", f"Files: {file_count}")
        
        # Filter valid files
        valid_files = [f for f in files if f.episode_number is not None and f.video_quality is not None]
        invalid_files = [f for f in files if f.episode_number is None or f.video_quality is None]
        
        if not valid_files:
            await update.message.reply_text(
                "❌ No valid files found. Check naming convention:\n"
                "`[S01-E07] Show Name [1080P] [Single].mkv`"
            )
            del self.user_sessions[user_id]
            return
        
        # Group by quality
        quality_groups = {480: [], 720: [], 1080: []}
        other_files = []
        
        for f in valid_files:
            if f.video_quality in quality_groups:
                quality_groups[f.video_quality].append(f)
            else:
                other_files.append(f)
        
        # Sort by episode number
        for quality in quality_groups:
            quality_groups[quality].sort(key=lambda x: x.episode_number)
        other_files.sort(key=lambda x: (x.episode_number, x.video_quality or 0))
        
        # Send sorted files
        await update.message.reply_text("🔄 Sending sorted files by quality...")
        dump_chat_id = self.dump_channels.get(user_id)
        
        for quality in [480, 720, 1080]:
            if quality_groups[quality]:
                await update.message.reply_text(
                    f"📺 **{quality}P QUALITY EPISODES**\n"
                    f"Sending {len(quality_groups[quality])} episodes...",
                    parse_mode='Markdown'
                )
                
                for video_file in quality_groups[quality]:
                    try:
                        if video_file.file_type == 'video':
                            await context.bot.send_video(
                                chat_id=update.effective_chat.id,
                                video=video_file.file_id,
                                caption=video_file.caption
                            )
                            if dump_chat_id:
                                await context.bot.send_video(
                                    chat_id=dump_chat_id,
                                    video=video_file.file_id,
                                    caption=video_file.caption
                                )
                                await asyncio.sleep(1)
                        else:
                            await context.bot.send_document(
                                chat_id=update.effective_chat.id,
                                document=video_file.file_id,
                                caption=video_file.caption
                            )
                            if dump_chat_id:
                                await context.bot.send_document(
                                    chat_id=dump_chat_id,
                                    document=video_file.file_id,
                                    caption=video_file.caption
                                )
                                await asyncio.sleep(1)
                    except Exception as e:
                        logger.error(f"Error sending file: {e}")
                        await update.message.reply_text(f"❌ Error sending: {video_file.filename}")
        
        if other_files:
            await update.message.reply_text(
                f"📺 **OTHER QUALITY EPISODES**\n"
                f"Sending {len(other_files)} episodes...",
                parse_mode='Markdown'
            )
            
            for video_file in other_files:
                try:
                    if video_file.file_type == 'video':
                        await context.bot.send_video(
                            chat_id=update.effective_chat.id,
                            video=video_file.file_id,
                            caption=video_file.caption
                        )
                        if dump_chat_id:
                            await context.bot.send_video(
                                chat_id=dump_chat_id,
                                video=video_file.file_id,
                                caption=video_file.caption
                            )
                            await asyncio.sleep(1)
                    else:
                        await context.bot.send_document(
                            chat_id=update.effective_chat.id,
                            document=video_file.file_id,
                            caption=video_file.caption
                        )
                        if dump_chat_id:
                            await context.bot.send_document(
                                chat_id=dump_chat_id,
                                document=video_file.file_id,
                                caption=video_file.caption
                            )
                            await asyncio.sleep(1)
                except Exception as e:
                    logger.error(f"Error sending file: {e}")
                    await update.message.reply_text(f"❌ Error sending: {video_file.filename}")
        
        # Generate summary
        summary = await self.generate_summary(valid_files, file_count, quality_groups, other_files)
        await update.message.reply_text(summary, parse_mode='Markdown')
        
        # Clear session
        del self.user_sessions[user_id]

    async def generate_summary(self, valid_files: List[VideoFile], total_files: int, 
                             quality_groups: Dict, other_files: List[VideoFile]) -> str:
        """Generate sorting summary"""
        processed_count = len(valid_files)
        summary = "✅ **SORTING COMPLETE**\n"
        summary += f"📊 {processed_count}/{total_files} files sorted\n\n"
        
        for quality in [480, 720, 1080]:
            if quality_groups[quality]:
                episodes = sorted([f.episode_number for f in quality_groups[quality]])
                episode_range = f"E{episodes[0]:02d}-E{episodes[-1]:02d}" if episodes else "None"
                summary += f"📺 {quality}p: {len(quality_groups[quality])} episodes ({episode_range})\n"
        
        if other_files:
            episodes = sorted([f.episode_number for f in other_files if f.episode_number])
            episode_range = f"E{episodes[0]:02d}-E{episodes[-1]:02d}" if episodes else "None"
            summary += f"📺 Other: {len(other_files)} episodes ({episode_range})\n"
        
        failed_count = total_files - processed_count
        if failed_count > 0:
            summary += f"\n❌ {failed_count} files failed processing"
        
        summary += "\n\n🎉 Files sent in order: 480p → 720p → 1080p"
        return summary

    # BULK UPLOAD COMMANDS

    async def delete_episode_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Delete specific episode"""
        if update.effective_user.id not in self.authorized_admins:
            await update.message.reply_text("❌ Admin access required")
            return

        if len(context.args) < 2:
            await update.message.reply_text("Usage: /delete <anime_id> <episode_id>")
            return

        if not self.db_pool:
            await update.message.reply_text("❌ Database not initialized")
            return

        try:
            anime_id = int(context.args[0])
            episode_id = int(context.args[1])

            async with self.db_pool.acquire() as conn:
                episode = await conn.fetchrow("""
                    SELECT e.*, a.anime_name
                    FROM episodes e
                    JOIN anime a ON e.anime_id = a.anime_id
                    WHERE e.anime_id = $1 AND e.id = $2
                """, anime_id, episode_id)

                if not episode:
                    await update.message.reply_text(f"❌ Episode not found (Anime ID: {anime_id}, Episode ID: {episode_id})")
                    return

                await conn.execute("DELETE FROM episodes WHERE id = $1", episode_id)

                await update.message.reply_text(
                    f"✅ **Deleted Episode:**\n"
                    f"• Anime: {episode['anime_name']}\n"
                    f"• Episode: [{episode['episode']}] [{episode['quality']}]\n"
                    f"• File: {episode['file_name']}"
                )
        except ValueError:
            await update.message.reply_text("❌ Invalid ID format. Both must be numbers.")
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}")


    async def search_episodes(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Search for episode URLs by anime_id, optional season, quality, episode parsed from url"""
        logger.info("Entered search_episodes")
        if not update.effective_user:
            logger.warning("Received /search command with no effective user (e.g., from a channel)")
            return
        args = context.args
        logger.info(f"Args received: {args}")
        if len(args) < 1:  # Requires at least anime_id
            await update.message.reply_text(
                "Usage: /search <anime_id> [season] [quality] [episode]\n\n"
                "Examples:\n"
                "• /search 1 (all episodes for anime_id 1)\n"
                "• /search 1 01 480 (season 01, quality 480p)\n"
                "• /search 1 01 480 11 (season 01, episode 11, quality 480p)",
                parse_mode='Markdown'
            )
            return
        if not self.db_pool:
            logger.error("Database pool not initialized")
            await update.message.reply_text("❌ Database not initialized")
            return
    
        def parse_video_filename(url_or_filename):
            """Parse video filename to extract season, episode, quality, and title information"""
            if url_or_filename.startswith('http'):
                decoded_url = urllib.parse.unquote(url_or_filename)
                filename = decoded_url.split('/')[-1].split('?')[0]
            else:
                filename = url_or_filename
    
            result = {
                'season': None,
                'episode': None,
                'quality': None,
                'title': None,
                'language': None,
                'format_type': None,
                'file_extension': None
            }
    
            if '.' in filename:
                result['file_extension'] = filename.split('.')[-1]
    
            season_episode_patterns = [
                r'S(\d+)-E(\d+)',
                r'S(\d+)E(\d+)',
                r'Season\s*(\d+)\s*Episode\s*(\d+)',
                r'(\d+)x(\d+)',
            ]
            for pattern in season_episode_patterns:
                match = re.search(pattern, filename, re.IGNORECASE)
                if match:
                    result['season'] = int(match.group(1))
                    result['episode'] = int(match.group(2))
                    break
    
            quality_patterns = [
                r'\[(\d+p?)\]',
                r'(\d{3,4}p)',
                r'(\d{3,4})',
            ]
            qualities_found = []
            for pattern in quality_patterns:
                matches = re.findall(pattern, filename, re.IGNORECASE)
                for match in matches:
                    clean_quality = re.sub(r'[^\d]', '', match)
                    if clean_quality and len(clean_quality) >= 3:
                        qualities_found.append(f"{clean_quality}p")
            if qualities_found:
                result['quality'] = qualities_found[0]
    
            language_patterns = [r'\b(Tam|Tamil|Tel|Telugu|Hin|Hindi|Eng|English|Mal|Malayalam|Kan|Kannada)\b']
            for pattern in language_patterns:
                match = re.search(pattern, filename, re.IGNORECASE)
                if match:
                    lang_code = match.group(1).lower()
                    lang_map = {
                        'tam': 'Tamil', 'tamil': 'Tamil',
                        'tel': 'Telugu', 'telugu': 'Telugu',
                        'hin': 'Hindi', 'hindi': 'Hindi',
                        'eng': 'English', 'english': 'English',
                        'mal': 'Malayalam', 'malayalam': 'Malayalam',
                        'kan': 'Kannada', 'kannada': 'Kannada'
                    }
                    result['language'] = lang_map.get(lang_code, match.group(1))
                    break
    
            format_patterns = [r'\[(Single|Dual|Multi)\]', r'\b(Single|Dual|Multi)\b']
            for pattern in format_patterns:
                match = re.search(pattern, filename, re.IGNORECASE)
                if match:
                    result['format_type'] = match.group(1).title()
                    break
    
            title = filename
            title = re.sub(r'\[\d+p?\]', '', title, flags=re.IGNORECASE)
            title = re.sub(r'\[S\d+-E\d+\]', '', title, flags=re.IGNORECASE)
            title = re.sub(r'S\d+E\d+', '', title, flags=re.IGNORECASE)
            title = re.sub(r'\[(Single|Dual|Multi|Tam|Tamil|Tel|Telugu|Hin|Hindi|Eng|English)\]', '', title, flags=re.IGNORECASE)
            title = re.sub(r'\.\w+$', '', title)
            title = re.sub(r'[_\-\.]+', ' ', title)
            title = re.sub(r'\s+', ' ', title).strip()
            title = re.sub(r'^[\[\]\-_\s]+|[\[\]\-_\s]+$', '', title)
            if title:
                result['title'] = title
    
            return result
    
        try:
            anime_id = int(args[0])
            season = args[1] if len(args) > 1 else None
            quality = args[2] if len(args) > 2 else None
            episode = args[3] if len(args) > 3 else None
            logger.info(f"Query params: anime_id={anime_id}, season={season}, quality={quality}, episode={episode}")
    
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch("SELECT url FROM episodes WHERE anime_id = $1", anime_id)
                logger.info(f"Query returned {len(rows)} rows initially: {[row['url'] for row in rows]}")
    
            if not rows:
                logger.info("No episodes found")
                await update.message.reply_text("No episodes found matching the criteria.")
                return
    
            # Filter and parse URLs
            filtered_rows = []
            for row in rows:
                parsed = parse_video_filename(row['url'])
                match = True
                if season and parsed['season'] != int(season):
                    match = False
                if episode and parsed['episode'] != int(episode):
                    match = False
                if quality and parsed['quality'] and parsed['quality'].lower() != f"{quality.lower()}p":
                    match = False
                if match:
                    filtered_rows.append(row)
    
            if not filtered_rows:
                logger.info("No episodes found after filtering")
                await update.message.reply_text("No episodes found matching the criteria.")
                return
    
            # Sort by episode
            filtered_rows.sort(key=lambda x: parse_video_filename(x['url'])['episode'] or 0)
            response = "\n".join(f"{i+1}) {row['url']}" for i, row in enumerate(filtered_rows))
            logger.info(f"Response: {response}")
            await update.message.reply_text(response, parse_mode=None)
        except ValueError as ve:
            logger.error(f"ValueError: {ve}")
            await update.message.reply_text("❌ Invalid input format. anime_id must be a number.")
        except Exception as e:
            logger.error(f"Error in search_episodes: {e}", exc_info=True)
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def anime_list_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show all anime with IDs"""
        if not self.db_pool:
            await update.message.reply_text("❌ Database not connected")
            return
        
        try:
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT a.anime_id, a.anime_name, COUNT(e.id) as episode_count
                    FROM anime a
                    LEFT JOIN episodes e ON a.anime_id = e.anime_id
                    GROUP BY a.anime_id, a.anime_name
                    ORDER BY a.anime_id
                """)
            
            if not rows:
                await update.message.reply_text("❌ No anime found in database")
                return
            
            message = "📺 **All Anime:**\n\n"
            for row in rows:
                message += f"🆔 **ID {row['anime_id']}:** {row['anime_name']}\n"
                message += f"   📊 {row['episode_count']} episodes\n\n"
            
            message += "💡 Use `/list <anime_id>` to see episodes"
            
            # Split long messages
            if len(message) > 4000:
                parts = message.split('\n\n')
                current_part = "📺 **All Anime:**\n\n"
                
                for part in parts[1:]:  # Skip the header
                    if len(current_part) + len(part) + 2 > 4000:
                        await update.message.reply_text(current_part, parse_mode='Markdown')
                        current_part = part + "\n\n"
                    else:
                        current_part += part + "\n\n"
                
                if current_part.strip():
                    current_part += "💡 Use `/list <anime_id>` to see episodes"
                    await update.message.reply_text(current_part, parse_mode='Markdown')
            else:
                await update.message.reply_text(message, parse_mode='Markdown')
                
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}")

    async def list_episodes_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """List episodes for specific anime"""
        if not context.args:
            await update.message.reply_text("Usage: `/list <anime_id>`")
            return
        
        if not self.db_pool:
            await update.message.reply_text("❌ Database not connected")
            return
        
        try:
            anime_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text("❌ Invalid anime ID. Must be a number.")
            return
        
        try:
            async with self.db_pool.acquire() as conn:
                anime = await conn.fetchrow(
                    "SELECT anime_name FROM anime WHERE anime_id = $1", anime_id
                )
                
                if not anime:
                    await update.message.reply_text(f"❌ No anime found with ID {anime_id}")
                    return
                
                episodes = await conn.fetch("""
                    SELECT id, episode, quality, file_name, url, timestamp
                    FROM episodes WHERE anime_id = $1
                    ORDER BY episode, quality
                """, anime_id)
                
                if not episodes:
                    await update.message.reply_text(f"❌ No episodes found for {anime['anime_name']}")
                    return
                
                message = f"📺 **{anime['anime_name']} (ID: {anime_id})**\n\n"
                messages = []
                
                for ep in episodes:
                    ep_msg = (f"🎬 **Episode ID {ep['id']}:**\n"
                             f"• [{ep['episode']}] [{ep['quality']}]\n"
                             f"• {ep['url']}\n"
                             f"• Added: {ep['timestamp'].strftime('%Y-%m-%d %H:%M')}\n\n")
                    
                    if len(message) + len(ep_msg) > 4000:
                        messages.append(message)
                        message = f"📺 **{anime['anime_name']} (continued)**\n\n" + ep_msg
                    else:
                        message += ep_msg
                
                if message.strip():
                    messages.append(message)
                
                for i, msg in enumerate(messages):
                    if i == len(messages) - 1:  # Last message
                        msg += f"💡 Use `/delete {anime_id} <episode_id>` to delete episodes"
                    await update.message.reply_text(msg, parse_mode='Markdown')
                    
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}")

    # CAPTION FORMATTING COMMANDS

    async def format_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Test caption formatting"""
        user_id = update.effective_user.id
        
        if not context.args:
            await update.message.reply_text(
                "🔧 **Format Tester**\n\n"
                "**Usage:** `/format YOUR TEXT HERE`\n\n"
                "**Examples:**\n"
                "• `/format [S01 E05] Naruto [1080p] Tamil.mkv`\n"
                "• `/format @Channel - Anime S01 EP12 [720] Tamil.mp4`",
                parse_mode='Markdown'
            )
            return
        
        test_text = ' '.join(context.args)
        formatted = self.parse_caption(test_text, user_id)
        
        await update.message.reply_text(
            f"🔧 **Format Test Result**\n\n"
            f"**Original:**\n`{test_text}`\n\n"
            f"**Formatted:**\n`{formatted}`",
            parse_mode='Markdown'
        )

    async def addprefix_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Add new prefix"""
        user_id = update.effective_user.id
        
        if not context.args:
            await update.message.reply_text(
                "➕ **Add New Prefix**\n\n"
                "**Usage:** `/addprefix YOUR_PREFIX`\n\n"
                "**Examples:**\n"
                "• `/addprefix /mirror -n`\n"
                "• `/addprefix /clone -n`\n"
                f"**Current prefixes:** {len(self.prefixes)}",
                parse_mode='Markdown'
            )
            return
        
        new_prefix = ' '.join(context.args).strip()
        
        if new_prefix in self.prefixes:
            await update.message.reply_text(
                f"⚠️ **Prefix already exists!**\n"
                f"**Prefix:** `{new_prefix}`",
                parse_mode='Markdown'
            )
            return
        
        self.prefixes.append(new_prefix)
        await self.save_user_settings(user_id)
        
        await update.message.reply_text(
            f"✅ **Prefix added!**\n"
            f"**New prefix:** `{new_prefix}`\n"
            f"**Total prefixes:** {len(self.prefixes)}",
            parse_mode='Markdown'
        )

    async def prefixlist_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show all prefixes"""
        if not self.prefixes:
            await update.message.reply_text(
                "❌ No prefixes configured!\n"
                "Use `/addprefix PREFIX` to add prefixes.",
                parse_mode='Markdown'
            )
            return
        
        prefix_list = "\n".join([f"{i+1}. `{prefix}`" for i, prefix in enumerate(self.prefixes)])
        
        await update.message.reply_text(
            f"📋 **Current Prefixes**\n\n"
            f"{prefix_list}\n\n"
            f"**Total:** {len(self.prefixes)} prefixes\n"
            f"**Rotation:** Every 3 messages",
            parse_mode='Markdown'
        )

    async def delprefix_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /delprefix command"""
        await self.load_user_settings(update.effective_user.id)
        if not context.args:
            if not self.prefixes:
                await update.message.reply_text(
                    "❌ **No prefixes to delete!**\n\n"
                    "Use `/addprefix PREFIX` to add prefixes first.",
                    parse_mode='Markdown',
                    reply_to_message_id=update.message.message_id
                )
                return
            prefix_list = "\n".join([f"{i+1}. `{prefix}`" for i, prefix in enumerate(self.prefixes)])
            await update.message.reply_text(
                f"➖ **Delete Prefix**\n\n"
                f"**Usage:** `/delprefix INDEX_NUMBER`\n\n"
                f"**Current prefixes:**\n{prefix_list}\n\n"
                f"**Example:** `/delprefix 3` (deletes 3rd prefix)",
                parse_mode='Markdown',
                reply_to_message_id=update.message.message_id
            )
            return
        try:
            index = int(context.args[0]) - 1  # Convert to 0-based index
            if index < 0 or index >= len(self.prefixes):
                await update.message.reply_text(
                    f"❌ **Invalid index!**\n\n"
                    f"**Valid range:** 1 to {len(self.prefixes)}\n"
                    f"**You entered:** {index + 1}",
                    parse_mode='Markdown',
                    reply_to_message_id=update.message.message_id
                )
                return
            deleted_prefix = self.prefixes.pop(index)
            await self.save_user_settings(update.effective_user.id)
            await update.message.reply_text(
                f"✅ **Prefix deleted successfully!**\n\n"
                f"**Deleted:** `{deleted_prefix}`\n"
                f"**Remaining:** {len(self.prefixes)} prefixes",
                parse_mode='Markdown',
                reply_to_message_id=update.message.message_id
            )
        except ValueError:
            await update.message.reply_text(
                f"❌ **Invalid number!**\n\n"
                "Please enter a valid number.\n"
                "**Example:** `/delprefix 2`",
                parse_mode='Markdown',
                reply_to_message_id=update.message.message_id
            )

    async def dumpchannel_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Set dump channel"""
        user_id = update.effective_user.id
        
        if not context.args:
            current_channel = self.dump_channels.get(user_id, "Not configured")
            await update.message.reply_text(
                f"📤 **Dump Channel Settings**\n\n"
                f"**Current:** `{current_channel}`\n\n"
                f"**Usage:**\n"
                f"• `/dumpchannel CHANNEL_ID` - Set channel\n"
                f"• `/dumpchannel reset` - Remove channel",
                parse_mode='Markdown'
            )
            return
        
        channel_input = ' '.join(context.args).strip()
        
        if channel_input.lower() == "reset":
            if user_id in self.dump_channels:
                del self.dump_channels[user_id]
            await self.save_user_settings(user_id)
            await update.message.reply_text("✅ Dump channel reset!")
            return
        
        self.dump_channels[user_id] = channel_input
        await self.save_user_settings(user_id)
        
        await update.message.reply_text(
            f"✅ **Dump channel set!**\n"
            f"**Channel:** `{channel_input}`",
            parse_mode='Markdown'
        )

    # ADMIN COMMANDS

    async def clear_db_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Clear entire database (admin only)"""
        user_id = update.effective_user.id
        
        if user_id not in self.authorized_admins:
            await update.message.reply_text("❌ Admin access required")
            return
        
        if not self.db_pool:
            await update.message.reply_text("❌ Database not connected")
            return
        
        keyboard = [[
            InlineKeyboardButton("✅ Yes, Clear All", callback_data="clear_db_confirm"),
            InlineKeyboardButton("❌ Cancel", callback_data="clear_db_cancel")
        ]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "⚠️ **DANGER: Clear Entire Database**\n\n"
            "This will permanently delete ALL anime and episodes!\n"
            "This action cannot be undone.\n\n"
            "Are you absolutely sure?",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )

    async def handle_clear_db_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle database clear confirmation"""
        query = update.callback_query
        await query.answer()
        
        if query.data == "clear_db_cancel":
            await query.edit_message_text("❌ Database clear cancelled.")
            return
        
        if query.data == "clear_db_confirm":
            user_id = query.from_user.id
            
            if user_id not in self.authorized_admins:
                await query.edit_message_text("❌ Unauthorized")
                return
            
            try:
                async with self.db_pool.acquire() as conn:
                    await conn.execute("DELETE FROM episodes")
                    await conn.execute("DELETE FROM anime")
                    await conn.execute("DELETE FROM user_settings")
                    await conn.execute("DELETE FROM bot_config")
                
                await query.edit_message_text("✅ **Database cleared completely**")
                logger.info(f"Database cleared by admin {user_id}")
                
            except Exception as e:
                await query.edit_message_text(f"❌ Error clearing database: {e}")

    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show database statistics"""
        if not self.db_pool:
            await update.message.reply_text("❌ Database not connected")
            return
        
        try:
            async with self.db_pool.acquire() as conn:
                anime_count = await conn.fetchval("SELECT COUNT(*) FROM anime")
                episode_count = await conn.fetchval("SELECT COUNT(*) FROM episodes")
                
                top_anime = await conn.fetch("""
                    SELECT a.anime_name, COUNT(e.id) as episode_count
                    FROM anime a
                    LEFT JOIN episodes e ON a.anime_id = e.anime_id
                    GROUP BY a.anime_id, a.anime_name
                    HAVING COUNT(e.id) > 0
                    ORDER BY episode_count DESC
                    LIMIT 5
                """)
                
                quality_stats = await conn.fetch("""
                    SELECT quality, COUNT(*) as count
                    FROM episodes
                    GROUP BY quality
                    ORDER BY count DESC
                """)
            
            message = f"📊 **Database Statistics**\n\n"
            message += f"📺 **Total Anime:** {anime_count}\n"
            message += f"🎬 **Total Episodes:** {episode_count}\n\n"
            
            if top_anime:
                message += "🏆 **Top Anime by Episodes:**\n"
                for anime in top_anime:
                    message += f"• {anime['anime_name']}: {anime['episode_count']}\n"
                message += "\n"
            
            if quality_stats:
                message += "🎥 **Quality Distribution:**\n"
                for stat in quality_stats:
                    message += f"• {stat['quality']}: {stat['count']} episodes\n"
            
            await update.message.reply_text(message, parse_mode='Markdown')
            
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}")

    # MESSAGE HANDLERS

    async def handle_media_with_caption(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle media messages with captions for formatting or sequencing"""
        user_id = update.effective_user.id  # Line 1249 in bot.py
        message = update.message
        original_caption = message.caption
        
        if not original_caption:
            return
        
        # Check if user is in sequencing mode
        if user_id in self.user_sessions:
            await self.handle_sequence_file(update, context)
            return
        
        # Otherwise, handle as caption formatting
        await self.load_user_settings(user_id)
        
        formatted_caption = self.parse_caption(original_caption, user_id)
        
        if formatted_caption and formatted_caption != original_caption:
            logger.info(f"Formatted caption: {formatted_caption}")
        
            # Reply with the formatted caption
            await message.reply_text(
                f"\n`{formatted_caption}`\n\n",
                parse_mode='Markdown',
                reply_to_message_id=message.message_id
            )
        else:
            await message.reply_text(
                "❌ **Parsing Failed**\n\n"
                "Could not parse the caption format.\n"
                "Try `/format YOUR_TEXT` to test or `/help` for supported formats.",
                parse_mode='Markdown',
                reply_to_message_id=message.message_id
            )
                
            username = update.effective_user.username or "Unknown"
            await self.log_action(context, user_id, username, "Caption formatted", original_caption[:50])
            await self.save_user_settings(user_id)

    async def handle_sequence_file(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle file upload during sequencing"""
        user_id = update.effective_user.id
        message = update.message
        
        if user_id not in self.user_sessions:
            return
        
        file_obj = None
        filename = "unknown_file"
        file_type = 'document'
        
        if message.document:
            file_obj = message.document
            filename = file_obj.file_name or "unknown_document"
            file_type = 'document'
        elif message.video:
            file_obj = message.video
            filename = file_obj.file_name or f"video_{file_obj.file_id[:8]}.mp4"
            file_type = 'video'
        else:
            return
        
        caption = message.caption or ''
        video_file = VideoFile(file_obj.file_id, filename, caption, file_type)
        self.user_sessions[user_id].append(video_file)
        
        if video_file.episode_number is not None and video_file.video_quality is not None:
            status = f"✅ Episode {video_file.episode_number}, Quality {video_file.video_quality}p"
        else:
            status = "⚠️ Could not parse episode/quality info"
        
        file_icon = "🎥" if file_type == 'video' else "📁"
        await message.reply_text(
            f"{file_icon} **File received:** `{filename}`\n{status}",
            parse_mode='Markdown'
        )

    async def handle_bulk_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle bulk upload messages"""
        text = update.message.text
        user_id = update.effective_user.id
        username = update.effective_user.username or "Unknown"
        
        # Check if it looks like a bulk upload message
        if not re.search(r'\d+\.\s*.*?https?://', text):
            return
        
        if not self.db_pool:
            await update.message.reply_text("❌ Database not connected")
            return
        
        entries = self.parse_bulk_message(text)
        if not entries:
            await update.message.reply_text("❌ Could not parse any entries from the message.")
            return
        
        stored, duplicates, anime_summary = await self.store_episodes(entries)
        
        status_msg = f"📊 **Processing Results:**\n"
        status_msg += f"• Parsed: {len(entries)} entries\n"
        status_msg += f"• Stored: {stored} new episodes\n"
        status_msg += f"• Duplicates: {duplicates}\n\n"
        
        if anime_summary:
            status_msg += "🆔 **Anime IDs:**\n"
            for anime_id, info in anime_summary.items():
                episode_count = len(info['episodes'])
                status_msg += f"• ID {anime_id}: {info['name']} ({episode_count} eps)\n"
        
        await update.message.reply_text(status_msg, parse_mode='Markdown')
        await self.log_action(context, user_id, username, "Bulk upload processed", f"{stored} stored, {duplicates} duplicates")

    async def handle_text_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle text messages for testing or bulk processing"""
        text = update.message.text
        user_id = update.effective_user.id
        
        # Skip commands
        if text.startswith('/'):
            return
        
        # Check if it's a bulk upload message first
        if re.search(r'\d+\.\s*.*?https?://', text):
            await self.handle_bulk_message(update, context)
            return
        
        # Otherwise treat as caption formatting test
        await self.load_user_settings(user_id)
        formatted = self.parse_caption(text, user_id)
        
        response_text = f"🔧 **Text Format Test**\n\n"
        response_text += f"**Original:**\n`{text}`\n\n"
        response_text += f"**Formatted:**\n`{formatted}`\n\n"
        response_text += "💡 Use `/name ANIME_NAME` to set fixed anime name"
        
        await update.message.reply_text(response_text, parse_mode='Markdown')
        await self.save_user_settings(user_id)


async def setup_bot_commands(application):
    """Set up bot command menu"""
    commands = [
        # Core commands
        BotCommand("start", "🚀 Start the unified bot"),
        BotCommand("help", "❓ Complete command guide"),
        BotCommand("status", "📊 Show bot status"),
        
        # Caption formatting
        BotCommand("name", "📝 Set/view anime name"),
        BotCommand("format", "🔧 Test caption formatting"),
        BotCommand("addprefix", "➕ Add new prefix"),
        BotCommand("delprefix", "❌ delete a prefix"),
        BotCommand("prefixlist", "📋 Show all prefixes"),
        BotCommand("dumpchannel", "📤 Set dump channel"),
        
        # File sequencing
        BotCommand("sequence", "📁 Start file collection"),
        BotCommand("endsequence", "✅ Sort and send files"),
        
        # Bulk upload management
        BotCommand("search", "🔍 Search episodes by anime_id, season, quality [episode]"),
        BotCommand("delete", "⛔ delete anime with IDs"),
        BotCommand("anime_list", "📺 Show all anime with IDs"),
        BotCommand("list", "📋 List episodes for anime"),
        BotCommand("stats", "📊 Database statistics"),
    ]
    
    await application.bot.set_my_commands(commands)


def main():
    """Main function for Render deployment"""
    # Get environment variables
    BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    PORT = int(os.getenv("PORT", 10000))  # Default to 10000

    if not BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN environment variable is required")
        return

    # Validate database URL
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        logger.error("DATABASE_URL environment variable is required")
        return

    logger.info("Starting Unified Anime Bot...")
    logger.info(f"Port: {PORT}")

    # Initialize bot
    bot = UnifiedAnimeBot()

    # Create application
    application = Application.builder().token(BOT_TOKEN).build()

    async def initialize_bot():
        """Initialize bot and database"""
        if not await bot.init_database():
            logger.error("Failed to initialize database")
            return False

        await setup_bot_commands(application)
        return True

    # Add all handlers
    application.add_handler(CommandHandler("start", bot.start_command))
    application.add_handler(CommandHandler("help", bot.help_command))
    application.add_handler(CommandHandler("status", bot.status_command))
    application.add_handler(CommandHandler("name", bot.name_command))
    application.add_handler(CommandHandler("format", bot.format_command))
    application.add_handler(CommandHandler("addprefix", bot.addprefix_command))
    application.add_handler(CommandHandler("delprefix", bot.delprefix_command))
    application.add_handler(CommandHandler("prefixlist", bot.prefixlist_command))
    application.add_handler(CommandHandler("dumpchannel", bot.dumpchannel_command))
    application.add_handler(CommandHandler("sequence", bot.sequence_command))
    application.add_handler(CommandHandler("endsequence", bot.endsequence_command))
    application.add_handler(CommandHandler("search", bot.search_episodes))
    application.add_handler(CommandHandler("delete", bot.delete_episode_command))
    application.add_handler(CommandHandler("anime_list", bot.anime_list_command))
    application.add_handler(CommandHandler("list", bot.list_episodes_command))
    application.add_handler(CommandHandler("stats", bot.stats_command))
    application.add_handler(CommandHandler("clear_db", bot.clear_db_command))
    application.add_handler(CallbackQueryHandler(bot.handle_clear_db_callback))

    # Media handlers
    application.add_handler(MessageHandler(
        filters.Document.ALL & filters.CAPTION,
        bot.handle_media_with_caption
    ))
    application.add_handler(MessageHandler(
        filters.VIDEO & filters.CAPTION,
        bot.handle_media_with_caption
    ))
    application.add_handler(MessageHandler(
        filters.PHOTO & filters.CAPTION,
        bot.handle_media_with_caption
    ))

    # Text message handler
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND,
        bot.handle_text_message
    ))

    async def startup():
        """Startup function for Render"""
        logger.info("Initializing unified anime bot...")
        if await initialize_bot():
            logger.info("Bot initialized successfully!")
            logger.info("Features active:")
            logger.info("- Auto Caption Formatting")
            logger.info("- File Sequencing & Sorting")
            logger.info("- Bulk Upload Processing")
            logger.info("- Database Storage & Management")
            logger.info("- Multi-format Support")
        else:
            logger.error("Bot initialization failed!")
            return False
        return True

    async def run_webhook():
        """Run bot with webhook for Render deployment"""
        try:
            await application.initialize()

            if not await startup():
                logger.error("Startup failed, exiting...")
                return

            await application.start()

            # Render provides HTTPS automatically
            webhook_url = f"https://{os.getenv('RENDER_EXTERNAL_HOSTNAME', 'all-in-one-xty5.onrender.com')}/webhook"
            logger.info(f"Setting webhook URL: {webhook_url}")
            try:
                await application.bot.set_webhook(
                    url=webhook_url,
                    allowed_updates=["message", "callback_query"]
                )
                logger.info("Webhook set successfully")
            except TelegramError as e:
                logger.error(f"Failed to set webhook: {e}")
                raise

            # Start FastAPI server
            from fastapi import FastAPI, Request
            import uvicorn

            app = FastAPI(title="Unified Anime Bot")

            @app.post("/webhook")
            async def webhook_handler(request: Request):
                """Handle webhook requests"""
                try:
                    update = Update.de_json(await request.json(), application.bot)
                    await application.process_update(update)
                    return {"status": "ok"}
                except Exception as e:
                    logger.error(f"Webhook error: {e}")
                    return {"status": "error", "message": str(e)}

            @app.get("/health")
            async def health_check():
                """Health check endpoint for Render"""
                logger.info("Health check endpoint accessed")
                return {
                    "status": "healthy",
                    "bot": "Unified Anime Bot",
                    "database": "connected" if bot.db_pool else "disconnected",
                    "features": [
                        "Caption Formatting",
                        "File Sequencing",
                        "Bulk Upload Processing",
                        "Database Management"
                    ]
                }

            @app.get("/")
            async def root():
                """Root endpoint"""
                return {
                    "message": "Unified Anime Bot is running!",
                    "status": "active",
                    "platform": "Render"
                }

            # Run FastAPI server
            config = uvicorn.Config(
                app=app,
                host="0.0.0.0",
                port=PORT,
                log_level="info"
            )
            server = uvicorn.Server(config)

            logger.info(f"Starting webhook server on 0.0.0.0:{PORT}")
            await server.serve()

        except Exception as e:
            logger.error(f"Webhook setup failed: {e}")
            raise  # Re-raise to debug instead of falling back to polling

    async def run_polling():
        """Run bot with polling (fallback mode)"""
        try:
            await application.initialize()

            if not await startup():
                logger.error("Startup failed, exiting...")
                return

            await application.start()
            logger.info("Starting polling mode...")
            await application.updater.start_polling(
                allowed_updates=["message", "callback_query"],
                drop_pending_updates=True
            )

            # Keep the bot running
            import signal
            import threading

            stop_event = threading.Event()

            def signal_handler(signum, frame):
                logger.info(f"Received signal {signum}, stopping bot...")
                stop_event.set()

            signal.signal(signal.SIGTERM, signal_handler)
            signal.signal(signal.SIGINT, signal_handler)

            # Wait for stop signal
            while not stop_event.is_set():
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Polling error: {e}")
        finally:
            logger.info("Stopping bot...")
            await application.updater.stop()
            await application.stop()
            await application.shutdown()
            if bot.db_pool:
                await bot.db_pool.close()
                logger.info("Database connections closed")

    # Check if we're running on Render
    if os.getenv('RENDER'):
        logger.info("Running on Render platform with webhook")
        asyncio.run(run_webhook())
    else:
        logger.info("Running in local/polling mode")
        asyncio.run(run_polling())


if __name__ == "__main__":
    main()
