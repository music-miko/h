# Authored By Certified Coders © 2025
# Modified for Team Arc
import os
import re
import aiofiles
import aiohttp
from PIL import Image, ImageDraw, ImageEnhance, ImageFilter, ImageFont
from youtubesearchpython.aio import VideosSearch
from config import YOUTUBE_IMG_URL
from AnnieXMedia.core.dir import CACHE_DIR 

# --- Layout Configuration ---
# Adjusted for a cleaner, professional card look
PANEL_W, PANEL_H = 800, 580  # Slightly wider/taller for better spacing
PANEL_X = (1280 - PANEL_W) // 2
PANEL_Y = 70
TRANSPARENCY = 230 # Increased opacity for a cleaner "solid" professional look
CORNER_RADIUS = 30

# Image Positioning
THUMB_W, THUMB_H = 580, 326 # 16:9 Aspect Ratio preserved nicely
THUMB_X = PANEL_X + (PANEL_W - THUMB_W) // 2
THUMB_Y = PANEL_Y + 40 # Padding from top

# Text Positioning
TEXT_CENTER_X = PANEL_X + (PANEL_W // 2)
TITLE_Y = THUMB_Y + THUMB_H + 25
META_Y = TITLE_Y + 45
BAR_Y = META_Y + 50

# Branding Configuration
BRAND_TEXT = "Powered By Team Arc"
BRAND_Y = PANEL_Y + PANEL_H - 50 # 50px from bottom

MAX_TITLE_WIDTH = 700

def trim_to_width(text: str, font: ImageFont.FreeTypeFont, max_w: int) -> str:
    ellipsis = "..."
    if font.getlength(text) <= max_w:
        return text
    for i in range(len(text) - 1, 0, -1):
        if font.getlength(text[:i] + ellipsis) <= max_w:
            return text[:i] + ellipsis
    return ellipsis

async def get_thumb(videoid: str) -> str:
    cache_path = os.path.join(CACHE_DIR, f"{videoid}_v5.png") # Changed version to force refresh
    if os.path.exists(cache_path):
        return cache_path

    # --- 1. Fetch Data ---
    results = VideosSearch(f"https://www.youtube.com/watch?v={videoid}", limit=1)
    try:
        results_data = await results.next()
        result_items = results_data.get("result", [])
        if not result_items:
            raise ValueError("No results found.")
        data = result_items[0]
        
        # Clean up title
        raw_title = data.get("title", "Unsupported Title")
        title = re.sub(r"\W+", " ", raw_title).title()
        
        thumbnail = data.get("thumbnails", [{}])[0].get("url", YOUTUBE_IMG_URL)
        duration = data.get("duration")
        views = data.get("viewCount", {}).get("short", "Unknown Views")
        channel = data.get("channel", {}).get("name", "Unknown Channel") 
    except Exception:
        title, thumbnail, duration, views, channel = "Unsupported Title", YOUTUBE_IMG_URL, None, "Unknown Views", "Unknown Channel"

    is_live = not duration or str(duration).strip().lower() in {"", "live", "live now"}
    duration_text = "LIVE" if is_live else duration or "00:00"

    # --- 2. Download Thumbnail ---
    thumb_path = os.path.join(CACHE_DIR, f"thumb{videoid}.png")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(thumbnail) as resp:
                if resp.status == 200:
                    async with aiofiles.open(thumb_path, "wb") as f:
                        await f.write(await resp.read())
    except Exception:
        return YOUTUBE_IMG_URL

    # --- 3. Graphics Composition ---
    
    # Fonts - Using fallbacks to ensure it works
    try:
        # Main Title
        font_title = ImageFont.truetype("AnnieXMedia/assets/thumb/font2.ttf", 36)
        # Regular Info
        font_reg = ImageFont.truetype("AnnieXMedia/assets/thumb/font.ttf", 22)
        # Branding Font (Slightly smaller, bold if possible, or same as reg)
        font_brand = ImageFont.truetype("AnnieXMedia/assets/thumb/font2.ttf", 20) 
    except OSError:
        font_title = font_reg = font_brand = ImageFont.load_default()

    # Base Background (Darkened Blur for contrast)
    base = Image.open(thumb_path).resize((1280, 720)).convert("RGBA")
    # Blur heavily for a smooth background
    bg = base.filter(ImageFilter.GaussianBlur(radius=15))
    # Darken background to make the panel pop
    bg = ImageEnhance.Brightness(bg).enhance(0.5)

    # Frosted Panel (Cleaner, White)
    overlay = Image.new("RGBA", (PANEL_W, PANEL_H), (255, 255, 255, TRANSPARENCY))
    mask = Image.new("L", (PANEL_W, PANEL_H), 0)
    ImageDraw.Draw(mask).rounded_rectangle((0, 0, PANEL_W, PANEL_H), CORNER_RADIUS, fill=255)
    
    # Paste Panel onto BG
    bg.paste(overlay, (PANEL_X, PANEL_Y), mask)

    # Draw Helper
    draw = ImageDraw.Draw(bg)

    # Place Thumbnail (Clean Rounded Corners)
    thumb_img = base.resize((THUMB_W, THUMB_H))
    thumb_mask = Image.new("L", (THUMB_W, THUMB_H), 0)
    ImageDraw.Draw(thumb_mask).rounded_rectangle((0, 0, THUMB_W, THUMB_H), 15, fill=255)
    bg.paste(thumb_img, (THUMB_X, THUMB_Y), thumb_mask)

    # --- 4. Text & Details ---

    # Title (Centered)
    trunc_title = trim_to_width(title, font_title, MAX_TITLE_WIDTH)
    title_w = font_title.getlength(trunc_title)
    draw.text((TEXT_CENTER_X - title_w / 2, TITLE_Y), trunc_title, fill=(20, 20, 20), font=font_title)

    # Meta Info (Views | Channel)
    meta_text = f"{views} • {channel}"
    meta_w = font_reg.getlength(meta_text)
    draw.text((TEXT_CENTER_X - meta_w / 2, META_Y), meta_text, fill=(60, 60, 60), font=font_reg)

    # Progress Bar (Simplified & Professional)
    BAR_WIDTH = 550
    BAR_HEIGHT = 8
    BAR_START_X = TEXT_CENTER_X - (BAR_WIDTH // 2)
    
    # Background Line
    draw.rounded_rectangle(
        (BAR_START_X, BAR_Y, BAR_START_X + BAR_WIDTH, BAR_Y + BAR_HEIGHT),
        radius=4, fill=(200, 200, 200)
    )
    # Progress Fill (Arbitrary 60% filled for aesthetics)
    FILL_WIDTH = int(BAR_WIDTH * 0.45) 
    draw.rounded_rectangle(
        (BAR_START_X, BAR_Y, BAR_START_X + FILL_WIDTH, BAR_Y + BAR_HEIGHT),
        radius=4, fill=(20, 20, 20) # Black/Dark Grey is often more pro than bright red
    )
    # Indicator Dot
    draw.ellipse(
        (BAR_START_X + FILL_WIDTH - 8, BAR_Y + (BAR_HEIGHT//2) - 8, 
         BAR_START_X + FILL_WIDTH + 8, BAR_Y + (BAR_HEIGHT//2) + 8),
        fill=(20, 20, 20)
    )

    # Time Stamps
    draw.text((BAR_START_X, BAR_Y + 20), "00:00", fill=(80, 80, 80), font=font_reg)
    
    dur_w = font_reg.getlength(duration_text)
    draw.text((BAR_START_X + BAR_WIDTH - dur_w, BAR_Y + 20), duration_text, fill=(20, 20, 20), font=font_reg)

    # --- 5. Branding (Powered By Team Arc) ---
    # Centered at bottom
    brand_w = font_brand.getlength(BRAND_TEXT)
    draw.text(
        (TEXT_CENTER_X - brand_w / 2, BRAND_Y), 
        BRAND_TEXT, 
        fill=(100, 100, 100), # Subtle grey for professional look
        font=font_brand
    )

    # Cleanup Temp File
    try:
        os.remove(thumb_path)
    except OSError:
        pass

    bg.save(cache_path)
    return cache_path
