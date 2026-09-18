import re
from typing import List, Optional
from thefuzz import fuzz

def normalize_tokens(name: str) -> List[str]:
    """
    Cleans punctuation and extracts alphanumeric tokens in lowercase.
    """
    clean = re.sub(r'[^a-zA-Z0-9]', ' ', name.lower())
    return [t for t in clean.split() if t]

def is_similar(name1: str, name2: str, threshold: int = 85) -> bool:
    """
    Checks if two product names represent the same or duplicate product.
    Prevents false positives where different variants/codes (e.g. 'Product A' vs 'Product B',
    'iPhone 15' vs 'iPhone 16', '500g' vs '1kg') share common prefix words.
    """
    if not name1 or not name2:
        return False
        
    n1 = name1.lower().strip()
    n2 = name2.lower().strip()
    
    if n1 == n2:
        return True
        
    t1 = normalize_tokens(name1)
    t2 = normalize_tokens(name2)
    
    if not t1 or not t2:
        return False
        
    # Same tokens regardless of word order (e.g. 'Green Apple' vs 'Apple Green')
    if sorted(t1) == sorted(t2):
        return True
        
    # If token count is different (e.g. 'Apple' vs 'Apple iPhone'), they are distinct products
    if len(t1) != len(t2):
        return False
        
    # For equal token counts, inspect differing tokens
    diffs = 0
    for w1, w2 in zip(sorted(t1), sorted(t2)):
        if w1 == w2:
            continue
        # If any differing token contains numbers or is a short identifier/letter (e.g. 'a' vs 'b', '1' vs '2')
        if any(c.isdigit() for c in w1 + w2) or len(w1) <= 3 or len(w2) <= 3:
            return False
        # If words are long, check if it's only a minor typo (fuzzy ratio >= threshold)
        if fuzz.ratio(w1, w2) < threshold:
            return False
        diffs += 1
        
    # Only similar if at most 1 minor typo in a long word occurred
    return diffs <= 1

def find_similar_name(target_name: str, existing_names: List[str], threshold: int = 85) -> Optional[str]:
    """
    Returns the first similar name found in the existing_names list, or None if no similar name is found.
    """
    if not target_name or not existing_names:
        return None
        
    for name in existing_names:
        if is_similar(target_name, name, threshold):
            return name
            
    return None
