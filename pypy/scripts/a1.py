# # LLD for Stock Exchange Order Matcher
# Problem Statement
# Design and implement a Stock Exchange Order Matching System that matches buy and sell orders for stocks in real-time. Your system should follow standard price-time priority matching logic.

# Functional Requirements
# Core Features:
# Place Orders: Support BUY and SELL orders with:
# Order ID
# Stock symbol (e.g., "AAPL", "GOOGL")
# Order type: LIMIT or MARKET
# Price (for LIMIT orders)
# Quantity
# Timestamp
# Match Orders:
# Match buy/sell orders based on price-time priority
# LIMIT orders: Match only at specified price or better
# MARKET orders: Match immediately at best available price
# Partial fills allowed (e.g., order for 100 shares can match multiple orders)
# Cancel Orders: Allow users to cancel pending orders
# Query Operations:
# Get order book for a stock (all pending buy/sell orders)
# Get order status (PENDING, PARTIALLY_FILLED, FILLED, CANCELLED)
# Get trade history
# Matching Rules
# Price-Time Priority:
# BUY orders: Highest price first, then earliest timestamp
# SELL orders: Lowest price first, then earliest timestamp
# Match condition: Buy price ≥ Sell price
# Execution price: Price of the order already in the book (maker's price)
# Example:
# Order Book for AAPL:
# BUY:  [100 @ $150, 50 @ $149, 30 @ $148]  (sorted high to low)
# SELL: [40 @ $150, 60 @ $151, 20 @ $152]   (sorted low to high)

# New SELL order: 70 @ $149
# → Matches 50 @ $150 (fills completely)
# → Matches 20 @ $150 (partial fill of 100 @ $150 order)
# → Remaining 80 @ $150 stays in book
# Non-Functional Requirements
# Performance: O(log n) order placement and O(1) best price lookup
# Thread-safety: Not required for this interview (assume single-threaded)
# Persistence: Not required (in-memory is fine)
# Data structures: Choose appropriate structures (hint: heaps/priority queues + hash maps)
# Expected Deliverables (60 minutes)
# Phase 1 (20 min): Core Data Structures
#  Define Order class
#  Define OrderBook class (per stock)
#  Define MatchingEngine class
#  Choose data structures for buy/sell sides
# Phase 2 (25 min): Core Operations
#  Implement place_order()
#  Implement matching logic
#  Handle partial fills
#  Return list of trades executed
# Phase 3 (15 min): Additional Features
#  Implement cancel_order()
#  Implement get_order_book()
#  Add basic tests/examples

# Implementation starts here

import heapq
from enum import Enum
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
import time

# ============ PHASE 1: Data Structures ============

class OrderType(Enum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"

class OrderSide(Enum):
    BUY = "BUY"
    SELL = "SELL"

class OrderStatus(Enum):
    PENDING = "PENDING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"

@dataclass
class Order:
    order_id: str
    symbol: str
    side: OrderSide
    order_type: OrderType
    price: Optional[float]  # None for MARKET orders
    quantity: int
    timestamp: float
    filled_quantity: int = 0
    status: OrderStatus = OrderStatus.PENDING
    
    @property
    def remaining_quantity(self) -> int:
        return self.quantity - self.filled_quantity
    
    def __repr__(self):
        return f"Order({self.order_id}, {self.side.value}, {self.quantity}@${self.price})"

@dataclass
class Trade:
    buy_order_id: str
    sell_order_id: str
    symbol: str
    price: float
    quantity: int
    timestamp: float
    
    def __repr__(self):
        return f"Trade(B:{self.buy_order_id}, S:{self.sell_order_id}, {self.quantity}@${self.price})"

# Wrapper for heap entries to maintain price-time priority
@dataclass(order=True)
class HeapEntry:
    priority: Tuple  # (price, timestamp) for sorting
    order: Order = field(compare=False)

class OrderBook:
    """Manages buy and sell orders for a single stock symbol"""
    
    def __init__(self, symbol: str):
        self.symbol = symbol
        # Max heap for buys (negate price for max heap behavior)
        self.buy_orders: List[HeapEntry] = []
        # Min heap for sells
        self.sell_orders: List[HeapEntry] = []
        # Track all orders by ID
        self.orders: Dict[str, Order] = {}
    
    def add_buy_order(self, order: Order):
        """Add buy order to max heap (highest price first, then earliest time)"""
        # Negate price for max heap, positive timestamp for FIFO
        priority = (-order.price, order.timestamp)
        entry = HeapEntry(priority=priority, order=order)
        heapq.heappush(self.buy_orders, entry)
        self.orders[order.order_id] = order
    
    def add_sell_order(self, order: Order):
        """Add sell order to min heap (lowest price first, then earliest time)"""
        priority = (order.price, order.timestamp)
        entry = HeapEntry(priority=priority, order=order)
        heapq.heappush(self.sell_orders, entry)
        self.orders[order.order_id] = order
    
    def get_best_buy(self) -> Optional[Order]:
        """Get best buy order (highest price) without removing"""
        self._clean_heap(self.buy_orders)
        return self.buy_orders[0].order if self.buy_orders else None
    
    def get_best_sell(self) -> Optional[Order]:
        """Get best sell order (lowest price) without removing"""
        self._clean_heap(self.sell_orders)
        return self.sell_orders[0].order if self.sell_orders else None
    
    def pop_best_buy(self) -> Optional[Order]:
        """Remove and return best buy order"""
        self._clean_heap(self.buy_orders)
        if self.buy_orders:
            return heapq.heappop(self.buy_orders).order
        return None
    
    def pop_best_sell(self) -> Optional[Order]:
        """Remove and return best sell order"""
        self._clean_heap(self.sell_orders)
        if self.sell_orders:
            return heapq.heappop(self.sell_orders).order
        return None
    
    def _clean_heap(self, heap: List[HeapEntry]):
        """Remove filled/cancelled orders from top of heap (lazy deletion)"""
        while heap and (heap[0].order.status in [OrderStatus.FILLED, OrderStatus.CANCELLED] or 
                        heap[0].order.remaining_quantity == 0):
            heapq.heappop(heap)
    
    def cancel_order(self, order_id: str) -> bool:
        """Mark order as cancelled (lazy deletion from heap)"""
        if order_id in self.orders:
            order = self.orders[order_id]
            if order.status == OrderStatus.PENDING or order.status == OrderStatus.PARTIALLY_FILLED:
                order.status = OrderStatus.CANCELLED
                return True
        return False
    
    def get_order_book_snapshot(self) -> Dict:
        """Return current state of order book"""
        self._clean_heap(self.buy_orders)
        self._clean_heap(self.sell_orders)
        
        buy_levels = []
        for entry in sorted(self.buy_orders, key=lambda x: x.priority):
            order = entry.order
            if order.remaining_quantity > 0:
                buy_levels.append({
                    'price': order.price,
                    'quantity': order.remaining_quantity,
                    'order_id': order.order_id
                })
        
        sell_levels = []
        for entry in sorted(self.sell_orders, key=lambda x: x.priority):
            order = entry.order
            if order.remaining_quantity > 0:
                sell_levels.append({
                    'price': order.price,
                    'quantity': order.remaining_quantity,
                    'order_id': order.order_id
                })
        
        return {
            'symbol': self.symbol,
            'buys': buy_levels,
            'sells': sell_levels
        }

# ============ PHASE 2: Matching Engine ============

class MatchingEngine:
    """Main engine that manages order books and executes matches"""
    
    def __init__(self):
        # Order books per symbol
        self.order_books: Dict[str, OrderBook] = {}
        # Trade history
        self.trades: List[Trade] = []
    
    def _get_or_create_book(self, symbol: str) -> OrderBook:
        """Get or create order book for symbol"""
        if symbol not in self.order_books:
            self.order_books[symbol] = OrderBook(symbol)
        return self.order_books[symbol]
    
    def place_order(self, order: Order) -> List[Trade]:
        """
        Place order and return list of trades executed.
        Main entry point for order placement.
        """
        book = self._get_or_create_book(order.symbol)
        trades = []
        
        if order.order_type == OrderType.LIMIT:
            trades = self._match_limit_order(order, book)
        else:  # MARKET order
            trades = self._match_market_order(order, book)
        
        # Add remaining quantity to book if not fully filled
        if order.remaining_quantity > 0 and order.order_type == OrderType.LIMIT:
            if order.side == OrderSide.BUY:
                book.add_buy_order(order)
            else:
                book.add_sell_order(order)
        
        # Update order status
        if order.filled_quantity == 0:
            order.status = OrderStatus.PENDING
        elif order.filled_quantity < order.quantity:
            order.status = OrderStatus.PARTIALLY_FILLED
        else:
            order.status = OrderStatus.FILLED
        
        return trades
    
    def _match_limit_order(self, order: Order, book: OrderBook) -> List[Trade]:
        """Match limit order against existing orders"""
        trades = []
        
        if order.side == OrderSide.BUY:
            # Match against sell orders
            while order.remaining_quantity > 0:
                best_sell = book.get_best_sell()
                
                # No match if no sell orders or price too high
                if not best_sell or best_sell.price > order.price:
                    break
                
                # Execute trade
                trade = self._execute_trade(order, best_sell, best_sell.price, book)
                trades.append(trade)
        
        else:  # SELL order
            # Match against buy orders
            while order.remaining_quantity > 0:
                best_buy = book.get_best_buy()
                
                # No match if no buy orders or price too low
                if not best_buy or best_buy.price < order.price:
                    break
                
                # Execute trade
                trade = self._execute_trade(best_buy, order, best_buy.price, book)
                trades.append(trade)
        
        return trades
    
    def _match_market_order(self, order: Order, book: OrderBook) -> List[Trade]:
        """Match market order at best available price"""
        trades = []
        
        if order.side == OrderSide.BUY:
            # Buy at best sell price
            while order.remaining_quantity > 0:
                best_sell = book.get_best_sell()
                if not best_sell:
                    break  # No more sell orders
                
                trade = self._execute_trade(order, best_sell, best_sell.price, book)
                trades.append(trade)
        
        else:  # SELL order
            # Sell at best buy price
            while order.remaining_quantity > 0:
                best_buy = book.get_best_buy()
                if not best_buy:
                    break  # No more buy orders
                
                trade = self._execute_trade(best_buy, order, best_buy.price, book)
                trades.append(trade)
        
        return trades
    
    def _execute_trade(self, buy_order: Order, sell_order: Order, 
                       price: float, book: OrderBook) -> Trade:
        """
        Execute trade between buy and sell order.
        Handles partial fills.
        """
        # Determine trade quantity (minimum of remaining quantities)
        trade_quantity = min(buy_order.remaining_quantity, sell_order.remaining_quantity)
        
        # Update order quantities
        buy_order.filled_quantity += trade_quantity
        sell_order.filled_quantity += trade_quantity
        
        # Update order status
        if buy_order.remaining_quantity == 0:
            buy_order.status = OrderStatus.FILLED
            book.pop_best_buy()  # Remove from heap
        
        if sell_order.remaining_quantity == 0:
            sell_order.status = OrderStatus.FILLED
            book.pop_best_sell()  # Remove from heap
        
        # Create trade record
        trade = Trade(
            buy_order_id=buy_order.order_id,
            sell_order_id=sell_order.order_id,
            symbol=buy_order.symbol,
            price=price,
            quantity=trade_quantity,
            timestamp=time.time()
        )
        
        self.trades.append(trade)
        return trade
    
    # ============ PHASE 3: Query Operations ============
    
    def cancel_order(self, order_id: str, symbol: str) -> bool:
        """Cancel a pending order"""
        if symbol in self.order_books:
            return self.order_books[symbol].cancel_order(order_id)
        return False
    
    def get_order_book(self, symbol: str) -> Dict:
        """Get current order book for symbol"""
        if symbol in self.order_books:
            return self.order_books[symbol].get_order_book_snapshot()
        return {'symbol': symbol, 'buys': [], 'sells': []}
    
    def get_order_status(self, order_id: str, symbol: str) -> Optional[str]:
        """Get status of an order"""
        if symbol in self.order_books:
            book = self.order_books[symbol]
            if order_id in book.orders:
                return book.orders[order_id].status.value
        return None
    
    def get_trade_history(self, symbol: Optional[str] = None) -> List[Trade]:
        """Get trade history, optionally filtered by symbol"""
        if symbol:
            return [t for t in self.trades if t.symbol == symbol]
        return self.trades


# ============ TEST CASES ============

def print_section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

def print_order_book(engine: MatchingEngine, symbol: str):
    book = engine.get_order_book(symbol)
    print(f"\n📊 Order Book for {symbol}:")
    print(f"  BUY orders: {len(book['buys'])}")
    for b in book['buys'][:5]:  # Show top 5
        print(f"    {b['quantity']} @ ${b['price']} (ID: {b['order_id']})")
    print(f"  SELL orders: {len(book['sells'])}")
    for s in book['sells'][:5]:  # Show top 5
        print(f"    {s['quantity']} @ ${s['price']} (ID: {s['order_id']})")

def run_tests():
    engine = MatchingEngine()
    
    # Test 1: Simple exact match
    print_section("Test 1: Simple Exact Match")
    b1 = Order("B1", "AAPL", OrderSide.BUY, OrderType.LIMIT, 150.0, 100, time.time())
    trades = engine.place_order(b1)
    print(f"Placed BUY order: {b1}")
    print(f"Trades executed: {len(trades)}")
    
    s1 = Order("S1", "AAPL", OrderSide.SELL, OrderType.LIMIT, 150.0, 100, time.time())
    trades = engine.place_order(s1)
    print(f"Placed SELL order: {s1}")
    print(f"✅ Trades executed: {trades}")
    print(f"B1 status: {b1.status.value}, S1 status: {s1.status.value}")
    
    # Test 2: Partial fill
    print_section("Test 2: Partial Fill")
    b2 = Order("B2", "AAPL", OrderSide.BUY, OrderType.LIMIT, 150.0, 100, time.time())
    engine.place_order(b2)
    print(f"Placed BUY order: {b2}")
    
    s2 = Order("S2", "AAPL", OrderSide.SELL, OrderType.LIMIT, 150.0, 50, time.time())
    trades = engine.place_order(s2)
    print(f"Placed SELL order: {s2}")
    print(f"✅ Trades executed: {trades}")
    print(f"B2 status: {b2.status.value}, filled: {b2.filled_quantity}/{b2.quantity}")
    print(f"S2 status: {s2.status.value}, filled: {s2.filled_quantity}/{s2.quantity}")
    print_order_book(engine, "AAPL")
    
    # Test 3: Price-time priority
    print_section("Test 3: Price-Time Priority")
    b3 = Order("B3", "AAPL", OrderSide.BUY, OrderType.LIMIT, 150.0, 50, time.time())
    engine.place_order(b3)
    print(f"Placed BUY order B3: 50 @ $150 (timestamp: {b3.timestamp})")
    
    time.sleep(0.01)  # Small delay for timestamp difference
    b4 = Order("B4", "AAPL", OrderSide.BUY, OrderType.LIMIT, 150.0, 50, time.time())
    engine.place_order(b4)
    print(f"Placed BUY order B4: 50 @ $150 (timestamp: {b4.timestamp})")
    
    s3 = Order("S3", "AAPL", OrderSide.SELL, OrderType.LIMIT, 150.0, 60, time.time())
    trades = engine.place_order(s3)
    print(f"Placed SELL order S3: 60 @ $150")
    print(f"✅ Trades executed: {trades}")
    print(f"B3 filled: {b3.filled_quantity} (should be 50 - complete)")
    print(f"B4 filled: {b4.filled_quantity} (should be 10 - partial)")
    print(f"Remaining in book:")
    print_order_book(engine, "AAPL")
    
    # Test 4: No match (price gap)
    print_section("Test 4: No Match - Price Gap")
    b5 = Order("B5", "GOOGL", OrderSide.BUY, OrderType.LIMIT, 148.0, 100, time.time())
    engine.place_order(b5)
    print(f"Placed BUY order: 100 @ $148")
    
    s4 = Order("S4", "GOOGL", OrderSide.SELL, OrderType.LIMIT, 150.0, 100, time.time())
    trades = engine.place_order(s4)
    print(f"Placed SELL order: 100 @ $150")
    print(f"✅ Trades executed: {len(trades)} (expected 0)")
    print_order_book(engine, "GOOGL")
    
    # Test 5: Market order
    print_section("Test 5: Market Order")
    s5 = Order("S5", "MSFT", OrderSide.SELL, OrderType.LIMIT, 150.0, 100, time.time())
    engine.place_order(s5)
    print(f"Placed SELL LIMIT order: 100 @ $150")
    
    b6 = Order("B6", "MSFT", OrderSide.BUY, OrderType.MARKET, None, 50, time.time())
    trades = engine.place_order(b6)
    print(f"Placed BUY MARKET order: 50 shares")
    print(f"✅ Trades executed: {trades}")
    print(f"Trade price: ${trades[0].price} (should match sell limit of $150)")
    
    # Test 6: Multiple partial fills
    print_section("Test 6: Multiple Partial Fills")
    b7 = Order("B7", "TSLA", OrderSide.BUY, OrderType.LIMIT, 200.0, 50, time.time())
    engine.place_order(b7)
    b8 = Order("B8", "TSLA", OrderSide.BUY, OrderType.LIMIT, 199.0, 30, time.time())
    engine.place_order(b8)
    b9 = Order("B9", "TSLA", OrderSide.BUY, OrderType.LIMIT, 198.0, 20, time.time())
    engine.place_order(b9)
    print(f"Placed 3 BUY orders at different prices")
    
    s6 = Order("S6", "TSLA", OrderSide.SELL, OrderType.LIMIT, 198.0, 70, time.time())
    trades = engine.place_order(s6)
    print(f"Placed SELL order: 70 @ $198")
    print(f"✅ Trades executed: {len(trades)}")
    for trade in trades:
        print(f"   {trade}")
    print(f"Total filled: {sum(t.quantity for t in trades)} shares")
    
    # Test 7: Cancel order
    print_section("Test 7: Cancel Order")
    b10 = Order("B10", "NFLX", OrderSide.BUY, OrderType.LIMIT, 500.0, 100, time.time())
    engine.place_order(b10)
    print(f"Placed BUY order B10: 100 @ $500")
    print(f"Order status: {engine.get_order_status('B10', 'NFLX')}")
    
    cancelled = engine.cancel_order("B10", "NFLX")
    print(f"✅ Cancelled: {cancelled}")
    print(f"Order status: {engine.get_order_status('B10', 'NFLX')}")
    
    # Test 8: Trade history
    print_section("Test 8: Trade History")
    all_trades = engine.get_trade_history()
    print(f"Total trades executed: {len(all_trades)}")
    aapl_trades = engine.get_trade_history("AAPL")
    print(f"AAPL trades: {len(aapl_trades)}")
    for trade in aapl_trades[:3]:
        print(f"  {trade}")

if __name__ == "__main__":
    run_tests()
