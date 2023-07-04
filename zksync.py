import asyncio
import aiohttp
from datetime import datetime, timezone
from dateutil.parser import parse
from collections import defaultdict

from rich.console import Console
from rich.table import Table

import pandas as pd
pd.set_option('display.unicode.east_asian_width', True) #设置输出右对齐

from wallet import *

RATIO = 5
METH_INDEX = 1
ETH_INDEX = 3
USDC_INDEX = 4
FEE_INDEX = 12

ZKS_ETH_CONTRACT = "0x0000000000000000000000000000000000000000"
ZKS_USDC_CONTRACT = "0x3355df6d4c9c3035724fd0e3914de96a5a83aaf4"
EMPTYCONTRACT = "0x0000000000000000000000000000000000008001".lower()

CONTRACTZKSTASK = (
    ["0x2da10a1e27bf85cedd8ffb1abbe97e53391c0295", "syncSwap"],
    ["0x39e098a153ad69834a9dac32f0fca92066ad03f4", "mav"],
    ["0x6C31035D62541ceba2Ac587ea09891d1645D6D07", "veSync"],
    ["0x9606eC131EeC0F84c95D82c9a63959F2331cF2aC", "izi"],
    ["0xCBE2093030F485adAaf5b61deb4D9cA8ADEAE509", "zns"],
    ["0x1BbD33384869b30A323e15868Ce46013C82B86FB", "eraLend"],
    ["0x1181D7BE04D80A8aE096641Ee1A87f7D557c6aeb", "eraLend"],
    ["0x8B791913eB07C32779a16750e3868aA8495F5964", "mute"],
    ["0x6e2B76966cbD9cF4cC2Fa0D76d24d5241E0ABC2F", "1inch"],    
    ["0xd29Aa7bdD3cbb32557973daD995A3219D307721f", "teva"],    
    ["0x50B2b7092bCC15fbB8ac74fE9796Cf24602897Ad", "teva"],   
    ["0xC5db68F30D21cBe0C9Eac7BE5eA83468d69297e6", "reactor"],
    ["0x04e9Db37d8EA0760072e1aCE3F2A219988Fdac29", "reactor"],     
)

CONTRACT2ZKSTASK = {x.lower(): y for x, y in CONTRACTZKSTASK}

base_columns = ["#", "m-eth", "m-tx", "eth", "usdc", "tx", "最后交易", "day", "week", "mon", "不同合约", "金额", "fee"]

def get_task_colums():
    task_colums = []
    seen = set()

    for _, y in CONTRACTZKSTASK:
        if y not in seen:
            seen.add(y)
            task_colums.append(y)
    return task_colums

task_colums = get_task_colums()

def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


async def get_eth_info(session, address, url="https://cloudflare-eth.com"):
    params = [
        {
            "jsonrpc": "2.0",
            "method": "eth_getBalance",
            "params": [address, "latest"],
            "id": 1
        },
        {
            "jsonrpc": "2.0",
            "method": "eth_getTransactionCount",
            "params": [address, "latest"],
            "id": 2
        }
    ]
    try:
        async with session.post(url, json=params) as res:
            results = await res.json()
        balance = round(int(results[0]["result"], 16) / 1e18, RATIO)
        tx_count = int(results[1]["result"], 16)
        return balance, tx_count
    except:
        return 0, 0

async def get_zks_base_info(session, address):
    url = f"https://zksync2-mainnet-explorer.zksync.io/address/{address}"

    async with session.get(url) as res:
        data = await res.json()    

    tx = data['info']["sealedNonce"]
    if tx < 10:
        tx = f"[red]{tx}[/red]"
    elif tx >= 100:
        tx = f"[bold][green]{tx}[/green][/bold]"
    elif tx >= 25:
        tx = f"[green]{tx}[/green]"
    
    balances = data["info"]["balances"]

    eth_blance = round(int(balances[ZKS_ETH_CONTRACT]["balance"], 16) / 1e18, RATIO) if ZKS_ETH_CONTRACT in balances else 0
    usdc_blance = round(int(balances[ZKS_USDC_CONTRACT]["balance"], 16) / 1e6, RATIO) if ZKS_USDC_CONTRACT in balances else 0

    return eth_blance, usdc_blance, tx

async def get_amount(address, list):
    total_amount = 0
    for data in list:
        if (data["from"].lower() == address.lower() and data["to"].lower() != EMPTYCONTRACT) or data["to"].lower() == address.lower() and data["from"].lower() != EMPTYCONTRACT:
            symbol = data['tokenInfo']['symbol']
            if symbol == "ETH":
                total_amount += int(data['amount'], 16) / 1e18 * float(data['tokenInfo']['usdPrice'])
            elif symbol == "USDC":
                total_amount += int(data['amount'], 16) / 1e6

    return total_amount

async def process_transactions(address, data_list, months, weeks, days, contracts):
    total_amount = total_fee = 0
    for data in data_list:
        if data['balanceChanges'][0]['from'].lower() == address.lower():
            total_amount += await get_amount(address, data["erc20Transfers"])

            tx_date = parse(data["receivedAt"]).replace(tzinfo=timezone.utc)
            months.add(tx_date.strftime("%Y-%m"))
            weeks.add(tx_date.strftime("%Y-%m-%W"))
            days.add(tx_date.strftime("%Y-%m-%d"))

            contracts.add(data["data"]["contractAddress"])

            total_fee += round(int(data["fee"], 16) / 1e18, 5)

    return total_amount, total_fee

async def get_zks_last_tx(date):
    datetime_object = parse(date).replace(tzinfo=timezone.utc)
    current_dateTime = datetime.now(timezone.utc)

    diff = current_dateTime-datetime_object
    diff_days = diff.days

    if diff_days > 14:
        return f"[red]{diff_days}d[/red]"

    if diff_days > 0:
        return f"{diff_days}d"
    
    diff_hours = diff.seconds // 3600
    if diff_hours > 0:
        return f"{diff_hours}h"
    
    diff_mins = diff.seconds // 60
    if diff_mins > 0:
        return f"{diff_mins}m"

    return f"{diff.seconds}s"

async def get_zks_info(session, address):
    url = f"https://zksync2-mainnet-explorer.zksync.io/transactions?limit=100&direction=older&accountAddress={address}"
    async with session.get(url) as res:
        data = await res.json()

    total = data["total"]
    last_tx_time = await get_zks_last_tx(data["list"][0]["receivedAt"])
    months, weeks, days, contracts = set(), set(), set(), set()
    
    if total > 100:
        total_amounts = total_fees = offset = 0
        from_block_number = data["list"][0]["blockNumber"]
        from_tx_index = data["list"][0]["indexInBlock"]

        while True:
            new_url = url + f"&fromBlockNumber={from_block_number}&fromTxIndex={from_tx_index}&offset={offset}"
            async with aiohttp.ClientSession() as session:
                async with session.get(new_url) as res:
                    new_data = await res.json()         

            total_amount, total_fee = await process_transactions(address, new_data["list"], months, weeks, days, contracts)
            total_amounts += total_amount
            total_fees += total_fee

            list_length = len(new_data["list"])
            if list_length < 100:
                break

            offset += list_length
    else:
        total_amounts, total_fees = await process_transactions(address, data["list"], months, weeks, days, contracts)

    total_amounts = round(total_amounts, 2)
    if total_amounts < 10000:
        total_amounts = f"[red]{total_amounts}[/red]"
    elif total_amounts >= 250000:
        total_amounts = f"[bold][green]{total_amounts}[/green][/bold]"        
    elif total_amounts >= 50000:
        total_amounts = f"[green]{total_amounts}[/green]"

    mon = len(months)
    if mon < 2:
        mon = f"[red]{mon}[/red]"
    elif mon >= 9:
        mon = f"[bold][green]{mon}[/green][/bold]"        
    elif mon >= 6:
        mon = f"[green]{mon}[/green]"        

    return last_tx_time, total_amounts, round(total_fees, 5), mon, len(weeks), len(days), len(contracts)

async def get_zks_task(session, address):
    url = f"https://zksync2-mainnet-explorer.zksync.io/transactions?limit=100&direction=older&accountAddress={address}"
    async with session.get(url) as res:
        data = await res.json()     

    tasks = defaultdict(int)
    for contract in [hashs["data"]["contractAddress"].lower() for hashs in data["list"]]:
        if contract in CONTRACT2ZKSTASK:
            tasks[CONTRACT2ZKSTASK[contract]] += 1

    return tasks

async def get_all_zks_info(session, address, idx):
    meth, mtx = await get_eth_info(session, address)
    zeth, zusdc, ztx = await get_zks_base_info(session, address)
    ltx, amount, fee, mon, week, day, contract = await get_zks_info(session, address)
    tasks = await get_zks_task(session, address)
    tasks_info = [tasks[task] if task in tasks else 0 for task in task_colums]    

    return [f"lu{idx+1}", meth, mtx, zeth, zusdc, ztx, ltx, day, week, mon, contract, amount, fee] + tasks_info


async def pd_show(args):
    index = args.idx

    async with aiohttp.ClientSession() as session:
        if index == 0:
            tasks = []

            for idx, address in enumerate(ADDRESSLIST):
                tasks.append(asyncio.create_task(get_all_zks_info(session, address, idx)))

            results = await asyncio.gather(*tasks)
            await session.close()
            
            df = pd.DataFrame(results, columns=base_columns+task_colums)          
            df.loc["总计"] = ["总计", df['m-eth'].sum(), "", df['eth'].sum(), df['usdc'].sum(), "", "", "", "", "", "", "", df['fee'].sum()] + ["" for _ in task_colums]
            df = df.to_string(index=False)    

        else:
            idx = index-1
            assert idx < len(ADDRESSLIST)

            address = ADDRESSLIST[idx]
            tasks = [asyncio.create_task(get_all_zks_info(session, address, idx))]
            results = await asyncio.gather(*tasks)
            await session.close()            
            
            df = pd.DataFrame(results, columns=base_columns+task_colums).to_string(index=False)   

    print(df)
    print("-"*200)
    print(f"时间: {str(datetime.now())[:19]}")

    if args.save:
        df.to_excel('zks-info.xlsx', index=False)

async def rich_show(args):
    index = args.idx

    table = Table(title=f"Zksync: {str(datetime.now())[:19]}")
    for col in base_columns+task_colums:
        table.add_column(col)

    async with aiohttp.ClientSession() as session:
        if index == 0:
            tasks = []

            for idx, address in enumerate(ADDRESSLIST):
                tasks.append(asyncio.create_task(get_all_zks_info(session, address, idx)))

            results = await asyncio.gather(*tasks)
            await session.close()


            meth = eth = usdc = fee = 0
            for result in results:
                meth += result[METH_INDEX]
                eth += result[ETH_INDEX]
                usdc += result[USDC_INDEX]
                fee += result[FEE_INDEX]

                table.add_row(*[str(r) for r in result])

            last_row = ["" for _ in range(len(base_columns+task_colums))]
            last_row[0] = "总计"
            last_row[METH_INDEX] = f"{round(meth, RATIO)}"
            last_row[ETH_INDEX] = f"{round(eth, RATIO)}"
            last_row[USDC_INDEX] = f"{round(usdc, RATIO)}"
            last_row[FEE_INDEX] = f"{round(fee, RATIO)}"
            table.add_row(*last_row)

        else:
            idx = index-1
            assert idx < len(ADDRESSLIST)

            address = ADDRESSLIST[idx]
            tasks = [asyncio.create_task(get_all_zks_info(session, address, idx))]
            results = await asyncio.gather(*tasks)
            await session.close()    

            for result in results:
                table.add_row(*[str(r) for r in result])

    Console().print(table)


async def main(args):
    if args.use_pd:
        await pd_show(args)
    else:
        await rich_show(args)

    
    
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-idx', type=int, default=0)
    parser.add_argument('-save', type=str2bool, default=False)
    parser.add_argument('-use_pd', type=str2bool, default=False)

    args = parser.parse_args()

    asyncio.run(main(args))
