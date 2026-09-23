# Adding a fabric

This page walks you from a geopackage of HRUs to a full set of PRMS parameters
for it, one step at a time. It is written for a collaborator who has the
geopackage and an account on the cluster, and who has not run this pipeline
before. It assumes nothing about your HPC experience.

**What you need before you start**

- One geopackage holding two layers: `nhru` (the HRU polygons) and `nsegment`
  (the stream segments the model routes on). `tjc` and `flaming_gorge` in
  `configs/base_config.yml` are two fabrics built exactly this way.
- The HRU layer has an integer id column that runs 1, 2, 3 … N with no gaps.
  Step 1 shows how to check.
- The geopackage is in EPSG:5070 (the CONUS Albers every other fabric uses).
- An account on the cluster in the `impd` project.

If your fabric comes as one geopackage per VPU that still needs merging, that
is a different procedure. See "Case B" under *Adding a new fabric* in
[HPC workflow](hpc-workflow.md).

Every command on this page is one line. Copy one line at a time. Wherever you
see `myfabric`, replace it with the short name you chose for your fabric
(letters, digits and underscores only, for example `flaming_gorge`). The
easiest way is to paste this page into a text editor, Find and Replace
`myfabric`, and copy from there.

## Read this first: what went wrong on the first outside run

Each of these happened to a careful scientist on their first day. Each one has
a rule, and the steps below follow the rules.

| What happened | The rule |
|---|---|
| Two clones of the repo sat side by side with the same name. The stale one was used, and the first command failed with "command not found". | Use the one live checkout, at the absolute path in Step 0. Nothing else. |
| `pixi init` and `pixi install` were run in the shared checkout. | Never run `pixi init`, `pixi install` or `pixi lock` in the shared checkout. The environment is already built. Every `pixi run` gets `--as-is`. |
| A shell variable was filled from a command that failed silently, so `mkdir "$DR/myfabric"` tried to create `/myfabric`. | No derived variables. Every path on this page is written out in full. |
| A variable set inside an interactive job was gone back on the login node. | Every step starts with the same four setup lines. Run them every time you open a terminal. |
| A two-line command with a trailing backslash was pasted from Teams as one line and failed. | Every command is one line. |
| The CONUS-sized SLURM defaults (384 GB, 18 hours) made a 1,700-HRU fabric wait a day in the queue. | Step 7 gives the small-fabric memory and time overrides. |

## The two paths on this cluster

| | Path |
|---|---|
| The live checkout (the code) | `/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2-params` |
| The data root (inputs and outputs) | `/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2` |

`nhgf/gfv2_param/gfv2-params` is not the repo. If you find yourself there,
you are in the wrong place.

## Step 0. Open a terminal in the right place

Run these four lines every time you open a new terminal, and again after any
interactive job ends. They cost nothing.

```bash
cd /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2-params
export PATH="$HOME/.pixi/bin:$PATH"
pixi --version
git branch --show-current
```

`pixi --version` must print 0.81 or newer. If it is older, run
`pixi self-update` once. If it says "command not found", install pixi in your
own home directory (see the pixi website) and run the `export PATH` line again.

`git branch --show-current` tells you which code you are running. Do not
switch branches; if it is not the branch you expected, ask the repo owner.

Two habits for the rest of the page:

- The login node is for light commands only: the ones on this page marked
  "login node is fine". Anything else goes through `sbatch` so it runs on a
  compute node.
- `squeue -u $USER` lists your running and waiting jobs. A job is done when it
  no longer appears there. `sacct -u $USER -X --format=JobID,JobName%30,State`
  shows how finished jobs ended: you want `COMPLETED`.

## Step 1. Look at your geopackage (login node is fine)

You need four facts from the file: the name of the HRU id column, the number of
HRUs, which VPU the fabric lies in, and whether it contains closed basins.
`ogrinfo` reads a geopackage's structure without loading it, so it is safe on
the login node.

Layers, feature counts and column names:

```bash
pixi run --as-is ogrinfo -so /path/to/your/myfabric.gpkg nhru | grep -E "Feature Count|Integer|Real|String"
pixi run --as-is ogrinfo -so /path/to/your/myfabric.gpkg nsegment | grep "Feature Count"
```

The first command prints `Feature Count: N` (your HRU count) and one line per
column. Pick the column that is your HRU id. It must be unique and run 1..N
with no gaps. A national NHM id (`nhm_id`, `nat_hru_id`) usually has gaps and
is the wrong choice; a local index (`model_hru_idx`, `hru_id`, `fg_hru_id`) is
usually right. Check it, replacing `IDCOL` with the column name:

```bash
pixi run --as-is ogrinfo -q /path/to/your/myfabric.gpkg -sql "SELECT MIN(IDCOL) min_id, MAX(IDCOL) max_id, COUNT(*) n, COUNT(DISTINCT IDCOL) n_unique FROM nhru"
```

You want `min_id = 1` and `max_id = n = n_unique`. That `max_id` is the
`expected_max_hru_id` you will type in Step 3. If the numbers disagree, fix the
geopackage before going on; the validator in Step 5 will refuse it otherwise.

Then decide two things you already know about your domain:

- **VPU.** Which of the 18 NHDPlus vector processing units the fabric lies in,
  as a two-digit string such as `"14"` (Upper Colorado) or `"12"`
  (Texas-Gulf). A fabric that spans several VPUs needs a `vpu` column on the
  `nhru` layer instead; ask the repo owner if that is your case.
- **Closed basins.** Does the domain contain terminal lakes or playas with no
  outlet? If not, leave the endorheic floor out in Step 3. An empty endorheic
  result is correct for a domain with no closed basin.

## Step 2. Copy the geopackage into the data root (login node is fine)

The file lives under the data root in a folder named after the fabric, not
under `input/`.

```bash
mkdir -p /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2/myfabric/fabric
cp /path/to/your/myfabric.gpkg /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2/myfabric/fabric/myfabric.gpkg
```

## Step 3. Register the fabric and fill in four values (login node is fine)

Run the Step 0 lines first if this is a new terminal. Then:

```bash
pixi run --as-is init-data-root --add-fabric myfabric
```

This appends a complete profile for `myfabric` to `configs/base_config.yml`
and creates its output folders under the data root. Open
`configs/base_config.yml` in an editor, find the `myfabric:` block at the
bottom, and fill in the four lines marked `TODO`:

| Line | What to type | Where it came from |
|---|---|---|
| `expected_max_hru_id: 0` | your HRU count N | `max_id` in Step 1 |
| `id_feature: nat_hru_id` | your id column name | Step 1 |
| `hru_gpkg: ".../myfabric.gpkg"` | your file name, if it differs | Step 2 |
| `vpu: "00"` | your VPU, in quotes, e.g. `"14"` | Step 1 |

Everything else in the block is already correct for this data root. Two
optional lines are commented out at the end: `min_onstream_comids` and
`min_endorheic_comids`. Leave them commented for a first run. Once the first
run has logged the real counts, you can set each floor a little below its
count so a future mis-wired run fails instead of silently producing a wrong
product.

`configs/base_config.yml` is a shared, tracked file. Tell the repo owner you
added a profile so it gets committed on a branch, rather than sitting as an
uncommitted change in the working tree.

## Step 4. Clip the flow-direction grid to your fabric (login node is fine)

The depression-storage steps run on a window of the CONUS flow-direction grid
that just covers your fabric. This writes that window as a small VRT file.

```bash
pixi run --as-is python scripts/clip_shared_to_fabric.py --fabric myfabric
```

It prints the output path, which ends in `myfabric/shared/myfabric_fdr.vrt`.

## Step 5. Validate the profile (login node is fine)

This is the last stop before anything expensive. It checks the profile against
the geopackage and prints one line per check.

```bash
pixi run --as-is python scripts/check_fabric_profile.py --fabric myfabric
```

Every line must say `PASS`. A `FAIL` line names what is wrong and where. Fix
the profile or the geopackage and run it again. The checks it makes are the
mistakes that otherwise fail silently: an id column with gaps, a wrong
`expected_max_hru_id`, a layer name that does not exist, a `vpu` still at its
placeholder, a declared input that is not on disk.

Also confirm the shared inputs every fabric reads are staged on this data root
(they are, on this cluster, but the check is free):

```bash
pixi run --as-is init-data-root --check --fabric myfabric
```

## Step 6. Split the fabric into batches (a SLURM job)

The parameter jobs work on spatial batches of HRUs. This job writes them.

```bash
FABRIC=myfabric sbatch slurm_batch/prepare_fabric.batch
```

It prints `Submitted batch job NNNNNN`. Wait until it no longer appears in
`squeue -u $USER`, then confirm it wrote a manifest:

```bash
cat /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2/myfabric/batches/manifest.yml
```

`n_features` must equal your HRU count.

## Step 7. Run the whole pipeline with one command

One driver submits every stage and chains each on the one before it: the
depression-storage rasters, the tiled depth computation, the zonal parameters,
the depression-storage ratios, the snow-depletion curves, and the final
gap-fill. You submit once and wait.

First set the list of zonal parameters this data root can build. Two of the
ten (`lulc_nlcd`, `lulc_foresce`) have no staged source here, and one failing
parameter cancels the rest of the chain, so this line is required:

```bash
export ZONAL_PARAMS="elevation slope aspect soils soil_moist_max lulc_nhm_v11 lulc_nalcms ssflux"
```

Always dry-run first. It prints the exact sequence of submissions and submits
nothing:

```bash
./slurm_batch/submit_fabric_rerun.sh --dry-run /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2/myfabric/batches myfabric
```

Then submit for real. Which line you use depends on the size of the fabric.

**A regional fabric** (up to a few tens of thousands of HRUs), so the jobs do
not sit in the queue for a day asking for CONUS-sized resources:

```bash
SBATCH_MEM_PER_NODE=64G SBATCH_TIMELIMIT=04:00:00 STAGE2_MEM=64G STAGE2_TIME=02:00:00 ./slurm_batch/submit_fabric_rerun.sh --force /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2/myfabric/batches myfabric
```

**A CONUS-scale fabric**, where the defaults (384 GB, 18 hours) are the right
numbers:

```bash
./slurm_batch/submit_fabric_rerun.sh --force /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2/myfabric/batches myfabric
```

What to expect:

- The driver prints one `Submitted` line per stage and returns. The chain runs
  unattended for hours; `squeue -u $USER` shows the stages waiting on each
  other as `(Dependency)`.
- If a stage fails, SLURM cancels everything after it, so nothing runs against
  incomplete inputs. Look in `logs/` and `slurm_batch/logs/` for the failed
  job's `.err` file, fix the cause, and resume from that stage:
  `./slurm_batch/submit_fabric_rerun.sh --from <stage> <batches path> myfabric`.
  The dry-run output lists the stage names.
- If you do not need snow-depletion curves, let the `snarea` stage fail (or
  cancel it) and resume with `--from fill`.

## Step 8. Where the results are

The parameter files are here, one CSV per parameter:

```
/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2/myfabric/params/merged/
```

`merged/<name>.csv` is the finished, gap-filled product. A quick sanity check
is the row count: each file should have your HRU count plus one header line.

```bash
wc -l /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2/myfabric/params/merged/nhm_elevation_params.csv
```

Two numbers worth reading in the job logs: the on-stream waterbody count from
the `segment_wbody` step and the endorheic count from the `endorheic` step. An
on-stream count near zero means the segments layer did not match the
waterbodies, and every waterbody became depression storage. Those two counts
are what the optional floors in Step 3 protect.

Optional figures:

```bash
FABRIC=myfabric sbatch slurm_batch/render_figures.batch
```

## If something goes wrong

| Symptom | Cause | Fix |
|---|---|---|
| `init-data-root: command not found` or `pixi: command not found` | Wrong directory, or pixi not on `PATH` | Run the Step 0 lines. Check `pwd` prints the live checkout path. |
| `mkdir: cannot create directory '/myfabric'` | A shell variable was empty | Use the full paths from this page, not a variable. |
| `No such file or directory` on a command that starts with two spaces | A backslash line continuation was pasted as one line | Retype the command as one line with no backslash. |
| `ERROR: no batch manifest at /myfabric/batches/manifest.yml` | Same empty-variable problem as above | Use the full batches path from Step 7. |
| `pixi run` fails to read `pixi.lock` | Your pixi is older than the lock file format | `pixi self-update`, then rerun Step 0. |
| A job waits in the queue for many hours | CONUS-sized resource request | Use the regional command in Step 7. |
| The validator says `expected_max_hru_id matches: FAIL` | The profile value is still `0`, or the id column has gaps | Redo Step 1 and Step 3. |
| The validator says `vpu resolves: FAIL` | `vpu` is still `"00"` | Set the real VPU in Step 3. |

## Related pages

- [Required profile keys](ARCHITECTURE.md#required-profile-keys) explains
  every line in the profile block.
- [HPC workflow](hpc-workflow.md) holds the full runbook, the per-stage
  detail of the one-command chain, and the per-VPU merge procedure ("Case B").
- [Parameter index](parameter_index.md) maps every output column to its PRMS
  parameter.
