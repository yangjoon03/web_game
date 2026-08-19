#!/usr/bin/env python3
"""
그림 맞추기 게임 서버
실행: python server.py
플레이어 접속: http://[이 PC의 IP]:8765/player.html
컨트롤러:      http://localhost:8765/controller.html
"""

import asyncio
import websockets
from websockets.asyncio.server import serve, ServerConnection
from websockets.http11 import Request, Response
from websockets.datastructures import Headers
import json, os, mimetypes
from pathlib import Path

# ── 상태 ──
clients: dict[ServerConnection, dict] = {}
game_state = {
    "phase": "lobby",
    "mode": None,
    "players": {},
    "round": 0,
    "max_rounds": 3,
    "current_drawer": None,
    "word": None,
    "timer": 60,
    "relay_data": [],
    "guesses_this_round": set(),
}

BASE_DIR = Path(__file__).parent

# ── 전송 유틸 ──
async def broadcast(message, exclude=None):
    data = json.dumps(message)
    targets = [ws for ws in clients if ws != exclude]
    if targets:
        await asyncio.gather(*[ws.send(data) for ws in targets], return_exceptions=True)

async def send_to(ws, message):
    try:
        await ws.send(json.dumps(message))
    except Exception:
        pass

async def send_to_controller(message):
    for ws, info in clients.items():
        if info.get("role") == "controller":
            await send_to(ws, message)

async def send_to_players(message):
    for ws, info in clients.items():
        if info.get("role") == "player":
            await send_to(ws, message)

def get_player_list():
    return [
        {"id": pid, "name": p["name"], "score": p["score"], "ready": p.get("ready", False)}
        for pid, p in game_state["players"].items()
    ]

# ── 메시지 처리 ──
async def handle_message(ws, message):
    try:
        data = json.loads(message)
    except Exception:
        return

    msg_type = data.get("type")
    info = clients.get(ws, {})

    if msg_type == "join":
        role = data.get("role", "player")
        name = data.get("name", "플레이어")
        pid = data.get("id", str(id(ws)))
        clients[ws] = {"role": role, "name": name, "id": pid}

        if role == "player":
            game_state["players"][pid] = {"name": name, "score": 0, "ready": False}
            await broadcast({"type": "player_joined", "players": get_player_list()})
            await send_to(ws, {"type": "joined", "id": pid, "game_state": {
                "phase": game_state["phase"],
                "mode": game_state["mode"],
                "players": get_player_list(),
                "round": game_state["round"],
                "max_rounds": game_state["max_rounds"],
            }})
        else:
            await send_to(ws, {"type": "joined", "role": "controller", "game_state": {
                "phase": game_state["phase"],
                "mode": game_state["mode"],
                "players": get_player_list(),
            }})

    elif msg_type == "start_game":
        mode = data.get("mode", "catchmind")
        game_state["mode"] = mode
        game_state["round"] = 0
        game_state["relay_data"] = []
        for pid in game_state["players"]:
            game_state["players"][pid]["score"] = 0
        if mode == "catchmind":
            await start_catchmind_round()
        else:
            await start_gartic_write_phase()

    elif msg_type == "set_word":
        word = data.get("word", "")
        game_state["word"] = word
        drawer_id = game_state["current_drawer"]
        for ws2, inf2 in clients.items():
            if inf2.get("id") == drawer_id:
                await send_to(ws2, {"type": "your_word", "word": word})
            elif inf2.get("role") == "player":
                await send_to(ws2, {"type": "round_start",
                    "drawer": drawer_id,
                    "drawer_name": game_state["players"].get(drawer_id, {}).get("name", ""),
                    "timer": game_state["timer"]})
        await send_to_controller({"type": "round_started",
            "drawer": drawer_id,
            "drawer_name": game_state["players"].get(drawer_id, {}).get("name", ""),
            "word": word,
            "timer": game_state["timer"]})

    elif msg_type == "draw":
        await broadcast(data, exclude=ws)

    elif msg_type == "clear_canvas":
        await broadcast({"type": "clear_canvas"})

    elif msg_type == "guess":
        pid = info.get("id")
        guess = data.get("text", "").strip()
        if game_state["phase"] == "drawing" and pid not in game_state["guesses_this_round"]:
            if guess == game_state["word"]:
                game_state["guesses_this_round"].add(pid)
                game_state["players"][pid]["score"] += 10
                drawer_id = game_state["current_drawer"]
                if drawer_id in game_state["players"]:
                    game_state["players"][drawer_id]["score"] += 5
                await broadcast({"type": "correct_guess",
                    "player_id": pid,
                    "player_name": game_state["players"][pid]["name"],
                    "players": get_player_list()})
                await send_to_controller({"type": "correct_guess",
                    "player_id": pid, "word": game_state["word"], "players": get_player_list()})
            else:
                await broadcast({"type": "chat",
                    "player_id": pid,
                    "player_name": game_state["players"].get(pid, {}).get("name", ""),
                    "text": guess})

    elif msg_type == "next_round":
        if game_state["mode"] == "catchmind":
            game_state["round"] += 1
            if game_state["round"] > game_state["max_rounds"]:
                game_state["phase"] = "lobby"
                await broadcast({"type": "game_ended", "players": get_player_list()})
            else:
                await start_catchmind_round()
        else:
            await start_gartic_write_phase()

    elif msg_type == "submit_sentence":
        pid = info.get("id")
        sentence = data.get("sentence", "")
        game_state["relay_data"].append({"type": "sentence", "author": pid,
            "author_name": game_state["players"].get(pid, {}).get("name", ""),
            "content": sentence})
        await send_to_controller({"type": "sentence_submitted", "player_id": pid, "sentence": sentence})
        submitted = [d for d in game_state["relay_data"] if d["type"] == "sentence"]
        if len(submitted) >= len(game_state["players"]):
            await start_gartic_draw_phase()

    elif msg_type == "submit_drawing":
        pid = info.get("id")
        image_data = data.get("image", "")
        game_state["relay_data"].append({"type": "drawing", "author": pid,
            "author_name": game_state["players"].get(pid, {}).get("name", ""),
            "content": image_data})
        await send_to_controller({"type": "drawing_submitted", "player_id": pid})
        submitted = [d for d in game_state["relay_data"] if d["type"] == "drawing"]
        if len(submitted) >= len(game_state["players"]):
            await start_gartic_guess_phase()

    elif msg_type == "submit_gartic_guess":
        pid = info.get("id")
        guess = data.get("guess", "")
        game_state["relay_data"].append({"type": "guess", "author": pid,
            "author_name": game_state["players"].get(pid, {}).get("name", ""),
            "content": guess})
        submitted = [d for d in game_state["relay_data"] if d["type"] == "guess"]
        if len(submitted) >= len(game_state["players"]):
            await show_gartic_results()

    elif msg_type == "end_game":
        game_state["phase"] = "lobby"
        await broadcast({"type": "game_ended", "players": get_player_list()})

    elif msg_type == "ready":
        pid = info.get("id")
        if pid in game_state["players"]:
            game_state["players"][pid]["ready"] = True
            await broadcast({"type": "player_ready", "player_id": pid, "players": get_player_list()})


# ── 게임 단계 ──
async def start_catchmind_round():
    player_ids = list(game_state["players"].keys())
    if not player_ids:
        return
    game_state["phase"] = "drawing"
    game_state["guesses_this_round"] = set()
    idx = (game_state["round"] - 1) % len(player_ids)
    game_state["current_drawer"] = player_ids[idx]
    drawer_name = game_state["players"][player_ids[idx]]["name"]
    await send_to_controller({
        "type": "need_word",
        "round": game_state["round"],
        "max_rounds": game_state["max_rounds"],
        "drawer_id": game_state["current_drawer"],
        "drawer_name": drawer_name,
    })
    await send_to_players({
        "type": "waiting_for_word",
        "drawer_name": drawer_name,
        "round": game_state["round"],
        "max_rounds": game_state["max_rounds"],
    })

async def start_gartic_write_phase():
    game_state["phase"] = "relay_write"
    game_state["round"] += 1
    game_state["relay_data"] = []
    await send_to_players({"type": "gartic_write", "timer": 60, "round": game_state["round"]})
    await send_to_controller({"type": "gartic_phase", "phase": "write"})

async def start_gartic_draw_phase():
    game_state["phase"] = "relay_draw"
    sentences = [d for d in game_state["relay_data"] if d["type"] == "sentence"]
    player_ids = list(game_state["players"].keys())
    for i, pid in enumerate(player_ids):
        sentence_idx = (i + 1) % len(sentences)
        sentence = sentences[sentence_idx]["content"] if sentences else "?"
        for ws, info in clients.items():
            if info.get("id") == pid:
                await send_to(ws, {"type": "gartic_draw", "sentence": sentence, "timer": 90})
    await send_to_controller({"type": "gartic_phase", "phase": "draw"})

async def start_gartic_guess_phase():
    game_state["phase"] = "relay_guess"
    drawings = [d for d in game_state["relay_data"] if d["type"] == "drawing"]
    player_ids = list(game_state["players"].keys())
    for i, pid in enumerate(player_ids):
        drawing_idx = (i + 1) % len(drawings)
        drawing = drawings[drawing_idx]["content"] if drawings else ""
        for ws, info in clients.items():
            if info.get("id") == pid:
                await send_to(ws, {"type": "gartic_guess", "image": drawing, "timer": 60})
    await send_to_controller({"type": "gartic_phase", "phase": "guess"})

async def show_gartic_results():
    game_state["phase"] = "results"
    await broadcast({"type": "gartic_results", "relay": game_state["relay_data"]})


# ── HTTP 파일 서빙 (websockets 17.x) ──
async def process_request(connection: ServerConnection, request: Request):
    # WebSocket 업그레이드 요청이면 None 반환 → websockets가 WS 핸드셰이크 처리
    if request.headers.get("Upgrade", "").lower() == "websocket":
        return None

    # 일반 HTTP 요청이면 파일 서빙
    path = request.path.split("?")[0]
    if path == "/" or path == "":
        path = "/controller.html"

    file_path = BASE_DIR / path.lstrip("/")
    if file_path.exists() and file_path.is_file():
        mime, _ = mimetypes.guess_type(str(file_path))
        body = file_path.read_bytes()
        headers = Headers([
            ("Content-Type", mime or "text/plain"),
            ("Content-Length", str(len(body))),
        ])
        return Response(200, "OK", headers, body)

    return Response(404, "Not Found", Headers([("Content-Type", "text/plain")]), b"Not Found")


# ── WebSocket 핸들러 ──
async def ws_handler(websocket: ServerConnection):
    clients[websocket] = {"role": "unknown", "name": "", "id": ""}
    try:
        async for message in websocket:
            await handle_message(websocket, message)
    finally:
        info = clients.pop(websocket, {})
        pid = info.get("id")
        if pid and pid in game_state["players"]:
            del game_state["players"][pid]
            await broadcast({"type": "player_left", "player_id": pid, "players": get_player_list()})


# ── 메인 ──
async def main():
    port = 8765
    import socket
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
    except Exception:
        local_ip = "127.0.0.1"

    print("=" * 50)
    print("🎨  그림 맞추기 게임 서버 시작!")
    print("=" * 50)
    print(f"  컨트롤러 (호스트):  http://localhost:{port}/controller.html")
    print(f"  플레이어 (친구들):  http://{local_ip}:{port}/player.html")
    print("=" * 50)
    print("  같은 Wi-Fi에 있는 친구들에게 플레이어 주소를 공유하세요!")
    print("  종료: Ctrl+C")
    print()

    async with serve(ws_handler, "0.0.0.0", port, process_request=process_request):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
