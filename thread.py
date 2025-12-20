import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED, ALL_COMPLETED
import threading
import logging

def square(n):
    """Calculate square of a number"""
    time.sleep(0.5)
    print(f"Calculating square of {n}")
    return n * n

def demo_map():
    """Using map() to process iterables"""
    print("\n=== MAP FUNCTION ===")
    
    numbers = [1, 2, 3, 4, 5, 6, 7, 8]
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        # map maintains order of inputs
        results = executor.map(square, numbers)
        
        # Results are returned in order
        for num, result in zip(numbers, results):
            print(f"{num}² = {result}")

if __name__ == "__main__":
    print("=" * 60)
    print("Python ThreadPoolExecutor: Complete Guide")
    print("=" * 60)
    
    demo_map()
    time.sleep(1)
