from fastapi import FastAPI

import state

app = FastAPI()


@app.get("/api/stats")
def get_stats():
    total_files = len(state.files)
    return {
        "total_files": total_files,
        "total_chunks": len(state.chunks),
        "completed": state.completed_files,
        "assigned": state.assigned_chunks,
        "pending": total_files - state.completed_files,
        "failed": state.failed_chunks,
        "active_workers": len(state.workers),
        "total_bytes": state.total_bytes
    }


@app.get("/api/leaderboard")
def get_leaderboard(limit: int = 25, offset: int = 0):
    response = []
    for leaderboard_id in state.current_leaderboard_order[offset:limit]:
        leaderboard_object = state.current_leaderboard[leaderboard_id]
        response.append({
            "discord_username": leaderboard_object.get_discord_username(),
            "avatar_url": leaderboard_object.get_avatar_url(),
            "total_chunks": leaderboard_object.get_total_chunks(),
            "total_bytes": leaderboard_object.get_total_bytes()
        })
    return response