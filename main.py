from warnings import filterwarnings
from random import randint, choice
from os import name as os_name
from loguru import logger
from time import sleep
import asyncio

from modules.utils import choose_mode
from modules.retry import DataBaseError
from modules import *
import settings


async def aclose_session(sol_wallet: SolWallet):
    try:
        await sol_wallet.browser.session.close()
        if sol_wallet.client:
            await sol_wallet.client.close()

    except Exception as err:
        logger.error(f'[-] Soft | {sol_wallet.address} | FAILED TO CLOSE SESSIONS: {err}')


def initialize_account(module_data: dict, group_data: dict = None):
    browser = Browser(
        db=db,
        proxy=module_data["proxy"],
        sol_address=module_data["sol_address"]
    )
    sol_wallet = SolWallet(
        privatekey=module_data["sol_pk"],
        encoded_pk=module_data["sol_encoded_pk"],
        label=module_data["label"],
        browser=browser,
        db=db,
    )
    return Pacifica(sol_wallet=sol_wallet, group_data=group_data)


async def run_module(mode: int, module_data: dict, sem: asyncio.Semaphore):
    async with address_locks[module_data["sol_address"]]:
        async with sem:
            try:
                pacifica = initialize_account(module_data)
                module_data["module_info"]["status"] = await pacifica.start(mode=mode)

            except DataBaseError:
                module_data = None
                raise

            except Exception as err:
                logger.error(f'[-] Soft | {pacifica.sol_wallet.address} | Account error | "{err.__class__.__name__}": {err}')
                await db.append_report(key=pacifica.sol_wallet.encoded_pk, text=str(err), success=False)

            finally:
                if type(module_data) == dict:
                    await aclose_session(pacifica.sol_wallet)
                    if mode == 1:
                        last_module = await db.remove_module(module_data=module_data)
                    else:
                        last_module = await db.remove_account(module_data=module_data)

                    reports = await db.get_account_reports(
                        key=pacifica.sol_wallet.encoded_pk,
                        label=pacifica.sol_wallet.label,
                        address=str(pacifica.sol_wallet.address),
                        last_module=last_module,
                        mode=mode,
                    )
                    if reports:
                        await TgReport().send_log(logs=reports)

                    if module_data["module_info"]["status"] is True:
                        await async_sleep(randint(*settings.SLEEP_AFTER_ACC))
                    else: await async_sleep(10)


async def run_pair(mode: int, group_data: dict, sem: asyncio.Semaphore):
    async with MultiLock([wallet_data["sol_address"] for wallet_data in group_data["wallets_data"]]):
        async with sem:
            pacifica_accounts = None
            try:
                pacifica_accounts = [
                    initialize_account(wallet_data, group_data=group_data)
                    for wallet_data in group_data["wallets_data"]
                ]
                group_data["module_info"]["status"] = await PairAccounts(
                    accounts=pacifica_accounts,
                    group_data=group_data
                ).run()

            except Exception as err:
                logger.error(f'[-] Group {group_data["group_number"]} | Global error | {err}')
                await db.append_report(key=group_data["group_index"], text=str(err), success=False)

            finally:
                if pacifica_accounts:
                    for pacifica in pacifica_accounts:
                        await aclose_session(pacifica.sol_wallet)
                await db.remove_group(group_data=group_data)

                reports = await db.get_account_reports(
                    key=group_data["group_index"],
                    label=f"Group {group_data['group_number']}",
                    address=None,
                    last_module=False,
                    mode=mode,
                )
                await TgReport().send_log(logs=reports)

                if group_data["module_info"]["status"] is True:
                    to_sleep = randint(*settings.SLEEP_AFTER_ACC)
                    logger.opt(colors=True).debug(f'[•] <white>Group {group_data["group_number"]}</white> | Sleep {to_sleep}s')
                    await async_sleep(to_sleep)
                else:
                    await async_sleep(10)


async def runner(mode: int):
    sem = asyncio.Semaphore(settings.THREADS)

    if mode in [2]:
        all_groups = db.get_all_groups()
        if all_groups != 'No more accounts left':
            await PriceListener.start_listening(
                proxy=choice(choice(all_groups)["wallets_data"])["proxy"]
            )

            await asyncio.gather(*[
                run_pair(group_data=group_data, mode=mode, sem=sem)
                for group_data in all_groups
            ])

    else:
        all_modules = db.get_all_modules(unique_wallets=mode in [3, 4])
        if all_modules != 'No more accounts left':
            await PriceListener.start_listening(choice(all_modules)["proxy"])
            await asyncio.gather(*[
                run_module(
                    mode=mode,
                    module_data=module_data,
                    sem=sem,
                )
                for module_data in all_modules
            ])

    logger.success(f'All accounts done.')
    return 'Ended'


if __name__ == '__main__':
    filterwarnings("ignore")
    if os_name == "nt":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        db = DataBase()

        while True:
            mode = choose_mode()

            match mode.type:
                case "database":
                    db.create_modules(mode=mode.soft_id)

                case "module":
                    if asyncio.run(runner(mode=mode.soft_id)) == 'Ended': break
                    print('')

        sleep(0.1)
        input('\n > Exit\n')

    except KeyboardInterrupt:
        pass

    except DataBaseError as err:
        logger.error(f'[-] Database | {err}')

    finally:
        logger.info('[•] Soft | Closed')
