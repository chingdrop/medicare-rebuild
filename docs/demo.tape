# Terminal recording for the README. Render from the repo root:
#
#   vhs docs/demo.tape
#
# Needs vhs (https://github.com/charmbracelet/vhs) plus its ttyd and ffmpeg
# dependencies, `uv sync` already run, and Docker running with the demo's SQL
# Server container up (`docker compose up -d mssql`; the first start takes a
# while, so start it before recording). The output is the real `make demo`
# run against generated synthetic data; nothing here is scripted text.
#
# vhs 0.12 with ffmpeg 9 exits 0 but silently writes no GIF (its own ffmpeg
# call fails). Workaround: point Output at a directory ("Output frames/") to
# get PNG frames, then assemble them yourself:
#
#   ffmpeg -framerate 50 -i frames/frame-text-%05d.png \
#     -framerate 50 -i frames/frame-cursor-%05d.png \
#     -filter_complex "[0][1]overlay,crop=iw:1720:0:0,scale=iw/2:-2:flags=lanczos,pad=iw+48:ih+48:24:24:color=0x171717,tpad=stop_mode=clone:stop_duration=8,fps=15,split[a][b];[a]palettegen[p];[b][p]paletteuse" \
#     docs/demo.gif
#
# The frames come out at 2x on a Retina display; the filter crops the empty rows
# below the output, scales back to 1x, adds padding and holds the final frame for
# 8 seconds so the result can be read.

Output docs/demo.gif

Set Shell "bash"
Set FontSize 14
Set Width 1060
Set Height 1000
Set Padding 20
Set TypingSpeed 55ms

# Start from a clean state with a short prompt (not recorded).
Hide
Type "export PS1='$ ' && cd $(git rev-parse --show-toplevel) && rm -rf demo_data demo_output && clear"
Enter
Show

Type "make demo"
Sleep 600ms
Enter

# The run takes about ten seconds; then hold on the result long enough to read.
Sleep 14s
Sleep 8s
