coupons = [
    {"CategoryName": "Comforter Sets", "CouponName": "Comforters Sale"},
    {"CategoryName": "Bedding", "CouponName": "Savings on Bedding"},
    {"CategoryName": "Bed & Bath", "CouponName": "Low price for Bed & Bath"}
]

categories = [
    {"CategoryName": "Comforter Sets", "CategoryParentName": "Bedding"},
    {"CategoryName": "Bedding", "CategoryParentName": "Bed & Bath"},
    {"CategoryName": "Bed & Bath", "CategoryParentName": None},
    {"CategoryName": "Soap Dispensers", "CategoryParentName": "Bathroom Accessories"},
    {"CategoryName": "Bathroom Accessories", "CategoryParentName": "Bed & Bath"},
    {"CategoryName": "Toy Organizers", "CategoryParentName": "Baby And Kids"},
    {"CategoryName": "Baby And Kids", "CategoryParentName": None}
]

def refresh_coupon_map(coupons, categories):
    coupon_map = {coupon["CategoryName"]: coupon["CouponName"] for coupon in coupons}
    parent_map = {category["CategoryName"]: category["CategoryParentName"] for category in categories}
    cat_to_coupon = {}

    def util(cat):
        if cat in cat_to_coupon:
            return cat_to_coupon[cat]
        if cat in coupon_map:
            result = coupon_map[cat]
        else:
            parent = parent_map.get(cat)
            if parent is None:
                result = None
            else:
                result = util(parent)
        cat_to_coupon[cat] = result
        return result
    
    for cat in parent_map:
        util(cat)
    
    return cat_to_coupon


res = refresh_coupon_map(coupons, categories)
print(res)


print(res["Comforter Sets"])        # "Comforters Sale"
print(res["Bedding"])               # "Savings on Bedding"
print(res["Bathroom Accessories"])  # "Low price for Bed & Bath"
print(res["Soap Dispensers"])       # "Low price for Bed & Bath"
print(res["Toy Organizers"])        # None
