import asyncio
import json
import logging
from bs4 import BeautifulSoup
import websockets

logging.basicConfig(
    filename="wss.log",
    # format="%(asctime)s %(levelname)s %(name)s %(message)s",
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.DEBUG,
)


class LoggerAdapter(logging.LoggerAdapter):
    """Add connection ID and client IP address to websockets logs."""

    def process(self, msg, kwargs):
        try:
            websocket = kwargs["extra"]["websocket"]
        except KeyError:
            return msg, kwargs
        return f"{websocket.id} {websocket.remote_address} {msg}", kwargs

async def handle_message(data):
    logging.debug("handle_message start")
    if data["results"]:
        #    if data["race_vehicle_id"] == 279623:
        # Parse the HTML content of "results" using BeautifulSoup
        soup = BeautifulSoup(data["results"], "html.parser")

        for row in soup.find_all("tr"):  # Loop through all rows (<tr>)
            # Extract data-race-vehicle-id
            race_vehicle_id = row.get("data-race-vehicle-id")

            # Find position span
            position_element = row.find(
                "span", class_="screen__track__results__position"
            )
            position = position_element.text.strip() if position_element else None

            # Extract name, race-best, last-lap, laps, delta
            cells = row.find_all("td")
            # cells[0] contains <span>, followed by text, followed by <span>
            # extract text between <spans> to name
            name_td = cells[0].contents[2] if cells else None
            name_kart = name_td.text.strip() if name_td else None
            name_kart_parts = name_kart.split(" - ") if name_kart else None
            name = name_kart_parts[0] if name_kart_parts else None
            kart = name_kart_parts[1] if name_kart_parts else None
            race_best = cells[1].text.strip() if len(cells) > 1 else None
            last_lap = cells[2].text.strip() if len(cells) > 2 else None
            laps = cells[3].text.strip() if len(cells) > 3 else None
            delta = cells[4].text.strip() if len(cells) > 4 else None

            # Print or store the extracted data (modify as needed)
            print(f"Vehicle ID: {race_vehicle_id}")
            print(f"Position: {position}")
            print(f"Name: {name}")
            print(f"Kart: {kart}")
            print(f"Best Time: {race_best}")
            print(f"Last Lap: {last_lap}")
            print(f"Laps: {laps}")
            print(f"Delta: {delta}")
            print("-" * 20)  # Separator between entries

async def main():
    logging.debug("main() starts")
    async with websockets.connect(
        "wss://kartcommander.motokaryplzen.cz/wss/screen_track",
        logger=LoggerAdapter(logging.getLogger("websockets.client"), None),
        # origin="https://kartcommander.motokaryplzen.cz",
        # extra_headers={
        #     #             "Cookie": "PHPSESSID=1g6p6krd4977bqf9k2vi1gq40e",
        #     "Pragma": "no-cache",
        #     "Cache-Control": "no-cache",
        #     "Accept-Encoding": "gzip, deflate, br, zstd",
        #     "Accept-Language": "en-US,en;q=0.9,cs;q=0.8"
        # },
        # user_agent_header="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    ) as websocket:
        logging.debug("wss connected")
        await websocket.send('{"track_id":6, "screen_id":3}')
        logging.debug("track_id sent")
        # await websocket.send("ping")  # Optional: Send a ping message
        async for message in websocket:
            logging.debug("wss msg received")
            try:
                data = json.loads(message)
                if "reload" in data:
                    logging.debug("reload received")
                elif "results" in data:
                    await handle_message(data)
                
            except json.JSONDecodeError:
                logging.error(f"Error decoding JSON message: {message}")


if __name__ == "__main__":
    while True:
        try:
            logging.debug("Before main() start")
            asyncio.run(main())
            logging.debug("After main() start")
        except Exception as e:
            logging.error(f"Error in main(): {e}")
            continue
