import json
from http.server import BaseHTTPRequestHandler, HTTPServer

PRODUTOS = [
    {"id": 1, "title": "Fjallraven - Foldsack No. 1 Backpack", "price": 109.95, "description": "Your perfect pack for everyday use.", "category": "men's clothing", "image": "https://fakestoreapi.com/img/81fPKd-2AYL._AC_SL1500_.jpg"},
    {"id": 2, "title": "Mens Casual Premium Slim Fit T-Shirts", "price": 22.3, "description": "Slim-fitting style.", "category": "men's clothing", "image": "https://fakestoreapi.com/img/71-3HjGNDUL._AC_SY879_.jpg"},
    {"id": 3, "title": "Mens Cotton Jacket", "price": 55.99, "description": "Great outerwear jackets.", "category": "men's clothing", "image": "https://fakestoreapi.com/img/71li-ujtlUL._AC_UX679_.jpg"},
    {"id": 4, "title": "Mens Casual Slim Fit", "price": 15.99, "description": "Casual slim fit shirt.", "category": "men's clothing", "image": "https://fakestoreapi.com/img/71YXzeOuslL._AC_UY879_.jpg"},
    {"id": 5, "title": "John Hardy Women's Legends Naga Bracelet", "price": 695.0, "description": "Inspired by the mythical water dragon.", "category": "jewelery", "image": "https://fakestoreapi.com/img/71pWzhdJNwL._AC_UL640_QL65_ML3_.jpg"},
    {"id": 6, "title": "Solid Gold Petite Micropave", "price": 168.0, "description": "Satisfaction guaranteed.", "category": "jewelery", "image": "https://fakestoreapi.com/img/61sbMiUnoGL._AC_UL640_QL65_ML3_.jpg"},
    {"id": 7, "title": "White Gold Plated Princess", "price": 9.99, "description": "Classic wedding engagement ring.", "category": "jewelery", "image": "https://fakestoreapi.com/img/71YAIFU48IL._AC_UL640_QL65_ML3_.jpg"},
    {"id": 8, "title": "Pierced Owl Rose Gold Plated", "price": 10.99, "description": "Rose gold plated earrings.", "category": "jewelery", "image": "https://fakestoreapi.com/img/51UDEzMJVpL._AC_UL640_QL65_ML3_.jpg"},
    {"id": 9, "title": "WD 2TB Elements Portable External Hard Drive", "price": 64.0, "description": "USB 3.0 and USB 2.0 compatibility.", "category": "electronics", "image": "https://fakestoreapi.com/img/61IBBVJvSDL._AC_SY879_.jpg"},
    {"id": 10, "title": "SanDisk SSD PLUS 1TB Internal SSD", "price": 109.0, "description": "Easy upgrade for faster boot up.", "category": "electronics", "image": "https://fakestoreapi.com/img/61U7T1koQqL._AC_SX679_.jpg"},
    {"id": 11, "title": "Silicon Power 256GB SSD", "price": 109.0, "description": "3D NAND flash applied.", "category": "electronics", "image": "https://fakestoreapi.com/img/71kWymZ+c+L._AC_SX679_.jpg"},
    {"id": 12, "title": "WD 4TB Gaming Drive", "price": 114.0, "description": "Expand your gaming experience.", "category": "electronics", "image": "https://fakestoreapi.com/img/61mtL65D4cL._AC_SX679_.jpg"},
    {"id": 13, "title": "Acer SB220Q bi 21.5 inches Full HD", "price": 599.0, "description": "Full HD widescreen IPS display.", "category": "electronics", "image": "https://fakestoreapi.com/img/81QpkIctqPL._AC_SX679_.jpg"},
    {"id": 14, "title": "Samsung 49-Inch CHG90 144Hz Curved Gaming Monitor", "price": 999.99, "description": "Super ultrawide screen QLED.", "category": "electronics", "image": "https://fakestoreapi.com/img/81Zt42ioCgL._AC_SX679_.jpg"},
    {"id": 15, "title": "BIYLACLESEN Women's 3-in-1 Snowboard Jacket", "price": 56.99, "description": "Detachable liner fabric.", "category": "women's clothing", "image": "https://fakestoreapi.com/img/51Y5NI-I5jL._AC_UX679_.jpg"},
    {"id": 16, "title": "Lock and Love Women's Removable Hooded Faux Leather Jacket", "price": 29.95, "description": "Faux leather material for style.", "category": "women's clothing", "image": "https://fakestoreapi.com/img/81XH0e8fefL._AC_UY879_.jpg"},
    {"id": 17, "title": "Rain Jacket Women Windbreaker", "price": 39.99, "description": "Lightweight perfect for trip.", "category": "women's clothing", "image": "https://fakestoreapi.com/img/71HblAHs5xL._AC_UY879_-2.jpg"},
    {"id": 18, "title": "MBJ Women's Solid Short Sleeve Boat Neck V", "price": 9.85, "description": "Lightweight fabric with great stretch.", "category": "women's clothing", "image": "https://fakestoreapi.com/img/71z3kpMAYsL._AC_UY879_.jpg"},
    {"id": 19, "title": "Opna Women's Short Sleeve Moisture", "price": 7.95, "description": "Machine wash breathable.", "category": "women's clothing", "image": "https://fakestoreapi.com/img/51eg55uWmdL._AC_UX679_.jpg"},
    {"id": 20, "title": "DANVOUY Womens T Shirt Casual Cotton Short", "price": 12.99, "description": "95% Cotton, 5% Spandex.", "category": "women's clothing", "image": "https://fakestoreapi.com/img/61pHAEJ4NML._AC_UX679_.jpg"},
]


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/products":
            body = json.dumps(PRODUTOS).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/error":
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b'{"error": "Simulated 500 Internal Server Error"}')
        else:
            self.send_response(404)
            self.end_headers()


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
