- Mac: uv pip install ".[mlx]" then uv run python main.py --run (auto-detects MLX)
- GPU box: uv pip install ".[cuda]" then uv run python main.py --run (auto-detects CUDA)
- Explicit: --device cuda or --device mlx to override

ssh -f -N -L 5432:localhost:5432 oci-pariyesana
uv run python main.py work