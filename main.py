import httpx
import pandas as pd
import os

#commented out zeta as under maintainence

#def fetch_zeta_orderbook_snap(symbol:str):

   # try:
       # data = httpx.get("https://dex-mainnet-webserver-ecs.zeta.markets/orderbooks?marketIndexes[]=137").json()
       # if 'orderbooks' in data and symbol in data['orderbooks']:
            # Extract asks and bids
          #   asks = data['orderbooks'][symbol][0]['asks']
           #  bids = data['orderbooks'][symbol][0]['bids']
            
            # Convert to DataFrames
            # asks_df = pd.DataFrame(asks).rename(columns={"price": "ask_price", "size": "ask_size"})
            # bids_df = pd.DataFrame(bids).rename(columns={"price": "bid_price", "size": "bid_size"})
            
            # Determine max length and extend DataFrames if necessary
            # max_len = max(len(bids_df), len(asks_df))
            
            # bids_df = bids_df.reindex(range(max_len))
            # asks_df = asks_df.reindex(range(max_len))

            # Reset index for clean merge
           #  bids_df = bids_df.reset_index(drop=True)
           #  asks_df = asks_df.reset_index(drop=True)

            # Merge DataFrames based on index
           #  orderbook_df = pd.concat([bids_df, asks_df], axis=1)
           #  orderbook_df['protocol_name'] = 'Zeta Markets'
          #   return orderbook_df
       #  else:
          #   print("Symbol not found in data.")
          #   return None

    # except Exception as e:
     #    print(e)
     #    pass

def fetch_hyperliquid_ob_snap(symbol:str):
    body = {
        "type":"l2Book",
        "coin":symbol
    }
    try:
        data = httpx.post("https://api.hyperliquid.xyz/info",json= body).json()
        
        bids_data = data['levels'][0]
        asks_data = data['levels'][1]
        
        # Convert to DataFrames
        bids_df = pd.DataFrame(bids_data)[['px', 'sz']]
        asks_df = pd.DataFrame(asks_data)[['px', 'sz']]
        
        # Rename columns and change data types
        bids_df.columns = ['bid_price', 'bid_size']
        asks_df.columns = ['ask_price', 'ask_size']

        bids_df['bid_price'] = bids_df['bid_price'].astype(float)
        bids_df['bid_size'] = bids_df['bid_size'].astype(float)

        asks_df['ask_price'] = asks_df['ask_price'].astype(float)
        asks_df['ask_size'] = asks_df['ask_size'].astype(float)
        
        # Determine max length and extend DataFrames if necessary
        max_len = max(len(bids_df), len(asks_df))
        
        bids_df = bids_df.reindex(range(max_len))
        asks_df = asks_df.reindex(range(max_len))

        # Reset index for clean merge
        bids_df = bids_df.reset_index(drop=True)
        asks_df = asks_df.reset_index(drop=True)

        # Merge DataFrames based on index
        orderbook_df = pd.concat([bids_df, asks_df], axis=1)
        orderbook_df['protocol_name'] = "Hyperliquid"
        
        return orderbook_df
    except Exception as e:
        print(e)
        pass

def fetch_vertex_ob_snap(symbol:str):
    try:
        data = httpx.get(f"https://prod.vertexprotocol-backend.com/api/v2/orderbook?ticker_id={symbol}-PERP_USDC&depth=25").json()
        # Extract bids and asks data
        bids_data = data['bids']
        asks_data = data['asks']
        
        # Convert to DataFrames
        bids_df = pd.DataFrame(bids_data, columns=['bid_price', 'bid_size'])
        asks_df = pd.DataFrame(asks_data, columns=['ask_price', 'ask_size'])

        # Determine max length and extend DataFrames if necessary
        max_len = max(len(bids_df), len(asks_df))
        
        bids_df = bids_df.reindex(range(max_len))
        asks_df = asks_df.reindex(range(max_len))

        # Reset index for clean merge
        bids_df = bids_df.reset_index(drop=True)
        asks_df = asks_df.reset_index(drop=True)

        # Merge DataFrames based on index
        orderbook_df = pd.concat([bids_df, asks_df], axis=1)
        orderbook_df['protocol_name'] = "Vertex"
        return orderbook_df

    except Exception as e:
        print(e)
        pass



#def fetch_and_save_zeta_orderbook_snap(symbol:str, filename:str):
   # orderbook_df = fetch_zeta_orderbook_snap(symbol)
   # orderbook_df['timestamp'] = pd.Timestamp.now()

   # if orderbook_df is not None:
      #  if os.path.exists(filename):
       #     existing_df = pd.read_csv(filename)
       #     combined_df = pd.concat([existing_df, orderbook_df], ignore_index=True)
       #     combined_df.to_csv(filename, index=False)
       # else:
          #  orderbook_df.to_csv(filename, index=False)

def fetch_and_save_hyperliquid_ob_snap(symbol:str, filename:str):
    orderbook_df = fetch_hyperliquid_ob_snap(symbol)
    orderbook_df['timestamp'] = pd.Timestamp.now()

    if orderbook_df is not None:
        if os.path.exists(filename):
            existing_df = pd.read_csv(filename)
            combined_df = pd.concat([existing_df, orderbook_df], ignore_index=True)
            combined_df.to_csv(filename, index=False)
        else:
            orderbook_df.to_csv(filename, index=False)

def fetch_and_save_vertex_ob_snap(symbol:str, filename:str):
    orderbook_df = fetch_vertex_ob_snap(symbol)
    orderbook_df['timestamp'] = pd.Timestamp.now()

    if orderbook_df is not None:
        if os.path.exists(filename):
            existing_df = pd.read_csv(filename)
            combined_df = pd.concat([existing_df, orderbook_df], ignore_index=True)
            combined_df.to_csv(filename, index=False)
        else:
            orderbook_df.to_csv(filename, index=False)


symbols = ["ETH", "BTC", "SOL", "ARB","TIA"]

def main():
    for symbol in symbols:
        #fetch_and_save_zeta_orderbook_snap(symbol, filename=f'zeta_{symbol}_orderbook_snap.csv')
        fetch_and_save_hyperliquid_ob_snap(symbol, filename=f'hyperliquid_{symbol}_orderbook_snap.csv')
        fetch_and_save_vertex_ob_snap(symbol, filename=f'vertex_{symbol}_orderbook_snap.csv')

if __name__ == "__main__":
    main()
