from typing import List, Optional
from thefuzz import fuzz

def is_similar(name1: str, name2: str, threshold: int = 85) -> bool:
    """
    Checks if two product names are similar based on thefuzz token_sort_ratio.
    Token sort ratio ignores word order (e.g., 'fruit juice' vs 'juice fruit').
    """
    if not name1 or not name2:
        return False
        
    # Convert to lowercase and strip extra spaces
    n1 = name1.lower().strip()
    n2 = name2.lower().strip()
    
    if n1 == n2:
        return True
        
    ratio = fuzz.token_sort_ratio(n1, n2)
    return ratio >= threshold

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
