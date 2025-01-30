import requests
from PIL import Image, ImageDraw, ImageFont
import time
import io

def add_product_watermark(image_url, download_path, text):
    start = time.time()
    
    try:
        response = requests.get(image_url, timeout=60)
        response.raise_for_status()
        image_data = response.content
        
        image = Image.open(io.BytesIO(image_data)).convert("RGB")
        if image is None:
            raise Exception("Could not download image " + image_url)

        watermarked = image.copy()
        draw = ImageDraw.Draw(watermarked)
        font_size = image.height // 30
        font_path = "/Users/sachinkumar/Documents/Testing/helvetica/Helvetica.ttf" 
         # Update this path to the location of your Helvetica font file
        font = ImageFont.truetype(font_path, font_size) # Using default font, you can replace it with any .ttf font
        
        text_position_black = (9, image.height - 13 - font_size)
        text_position_white = (11, image.height - 11 - font_size)

        draw.text(text_position_black, text, font=font, fill="black")
        draw.text(text_position_white, text, font=font, fill="white")
        watermarked.save(download_path, format="JPEG")
        
        time_taken = time.time() - start
        if time_taken > 10:
            print(f"time to add_product_watermark : {time_taken} seconds, for productId {text}")
    except Exception as e:
        print(f"Error in add_product_watermark: {e}")
    finally:
        image.close()
        watermarked.close()

# Example usage
image_url = "https://m.media-amazon.com/images/I/61X+Yx14SEL._SY879_.jpg"
download_path = "watermark.jpg"
text = "1299303030"
add_product_watermark(image_url, download_path, text)
