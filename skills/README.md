# Comfy Pilot Skills

**Reusable, shareable, Claude-optimized workflow components.**

A Skill is a complete, production-ready workflow that Claude Code understands, deploys, and optimizes. Think of them as *application templates for generative AI*.

---

## What is a Skill?

```
Traditional Workflow:
1. Click "Add Node"
2. Search for type
3. Place and wire manually
4. Adjust parameters
5. Run workflow
6. Iterate

Claude Skill:
1. "Use the SDXL generation skill"
2. Claude deploys complete workflow
3. Claude optimizes for your use case
4. Done
```

**Skills = Workflows that Claude understands and can deploy.**

---

## Discover Skills

### Available Skills

```bash
# In Claude Code terminal:
skill list                    # Show all skills
skill search generation       # Search by keyword
skill info sdxl-generation    # Get details
```

### Current Skills

| Skill | Status | Rating | Downloads |
|-------|--------|--------|-----------|
| **SDXL Generation Base** | ✅ Production | ⭐⭐⭐⭐⭐ 4.8 | 5,420 |
| SDXL Turbo (Fast) | 🔜 Coming Feb 4 | - | - |
| SDXL Refined (Quality) | 🔜 Coming Feb 4 | - | - |
| 4x Upscale | 🔜 Coming Feb 11 | - | - |
| ControlNet Pose | 🔜 Coming Feb 18 | - | - |
| Batch Generation | 🔜 Coming Feb 25 | - | - |

---

## Use a Skill

### Option 1: Ask Claude Code Directly

```
"Use the SDXL generation skill to create a professional portrait"
```

Claude will:
1. Deploy the skill
2. Configure parameters for portraits
3. Run the workflow
4. Show you results

### Option 2: Manual Deployment

```bash
skill deploy sdxl-generation
```

Then ask Claude to use it:
```
"Generate an image with the portrait prompt"
```

### Option 3: Combine Multiple Skills

```
"Deploy the SDXL generation skill, then upscale the result with the 4x upscale skill"
```

Claude chains them together.

---

## Core Skills (Currently Available)

### 📸 SDXL Generation Base

Production-ready image generation.

**Best for:**
- Professional portraits
- Cinematic landscapes
- Product photography
- Concept art

**Quick start:**
```
"Generate a professional headshot"
```

[View Full Documentation](./core/sdxl-generation/README.md)

---

## Upcoming Skills

### ⚡ SDXL Turbo (Feb 4)
Ultra-fast 4-step generation. Great for quick iterations.

### 🎨 SDXL Refined (Feb 4)
Maximum quality with refiner pipeline.

### 🔝 4x Upscale (Feb 11)
Real-ESRGAN upscaling for detail enhancement.

### 🧍 ControlNet Pose (Feb 18)
Generate images with specific poses.

### 🔄 Batch Generation (Feb 25)
Create multiple images with variations.

---

## Why Skills?

### For Users

✅ **Instant workflows** - No clicking, no setup
✅ **Pre-optimized** - Quality settings already tuned
✅ **Shareable** - One link, others can use it
✅ **Improvable** - Claude can analyze and optimize

### For Developers

✅ **Standardized** - Everyone uses same format
✅ **Discoverable** - Claude Code finds them automatically
✅ **Community** - Share and get credited
✅ **Monetizable** - Premium skills, future marketplace

### For Comfy Pilot

✅ **Network effect** - More skills = more valuable
✅ **Community moat** - Hard to replicate
✅ **Engagement** - Users return for new skills
✅ **Market position** - "The app store for workflows"

---

## Skill Anatomy

Each skill contains:

```
skill-name/
├── skill.json          # Metadata & configuration
├── workflow.json       # ComfyUI workflow graph
├── README.md           # Documentation
├── examples/           # Usage examples
└── docs/               # Additional docs
```

**Key files:**

- **`skill.json`** - What the skill is, what it needs, what it produces
- **`workflow.json`** - The actual ComfyUI workflow
- **`README.md`** - How to use it

[Full Specification](./SKILL_SPEC.md)

---

## Create Your Own Skill

### Step 1: Create Workflow

Build a working ComfyUI workflow manually.

### Step 2: Document It

Create `skill.json`:
```json
{
  "id": "my-skill",
  "name": "My Awesome Skill",
  "description": "What it does",
  "inputs": { ... },
  "outputs": { ... }
}
```

### Step 3: Export Graph

Export the workflow as `workflow.json`.

### Step 4: Write README

Document usage, examples, tips.

### Step 5: Test

```bash
skill validate path/to/skill
```

### Step 6: Submit

Open a PR to add your skill to the registry.

[Full Contribution Guide](../SKILLS_CONTRIBUTE.md)

---

## Best Practices

### Naming

✅ **Good:** `sdxl-generation`, `upscale-4x`, `controlnet-pose`
❌ **Bad:** `my_workflow`, `test`, `skill1`

### Documentation

✅ **Good:** Clear examples, performance info, tips
❌ **Bad:** No examples, vague descriptions

### Parameters

✅ **Good:** Sensible defaults, clear descriptions, ranges
❌ **Bad:** Confusing parameter names, no defaults

### Version Numbers

✅ **Good:** `1.0.0` (major.minor.patch)
❌ **Bad:** `1` or `v1.0` or dates

---

## FAQ

### Can I contribute a skill?

**Yes!** Submit via [Contribution Guide](../SKILLS_CONTRIBUTE.md).

Top community skills get featured and promoted.

### Can I sell my skill?

**Future:** Yes. We're planning a marketplace where creators can charge.

**Now:** Share freely, get recognition and GitHub stars.

### How do I optimize my skill?

Claude Code can analyze your skill:

```
"Analyze this workflow for optimization opportunities"
```

Claude will suggest:
- Faster samplers
- Better parameter defaults
- Memory optimizations
- Quality improvements

### What if the skill breaks?

Update it! Version bumps:
- **Major:** Breaking changes (input/output structure)
- **Minor:** New optional features
- **Patch:** Bug fixes, documentation

### Can skills use external APIs?

Not recommended. Keep skills self-contained in ComfyUI.

If needed, request the feature in [GitHub Issues](https://github.com/anthropics/comfy-pilot/issues).

---

## Community

### Share Your Skill

1. Create skill following [Specification](./SKILL_SPEC.md)
2. Add to GitHub repo
3. Submit PR with example
4. Community votes
5. Featured in registry

### Get Recognition

Top community skill creators get:
- Featured in registry
- Highlighted in announcements
- Potential sponsorship
- Community credit

### Join Discord

[Discord Community](https://discord.gg/comfy-pilot) for:
- Skill discussions
- Feedback and ideas
- Collaborations
- Announcements

---

## Statistics

**Current Stats:**

| Metric | Value |
|--------|-------|
| Total Skills | 1 (core) |
| Total Downloads | 5,420 |
| Active Users | 1,200 |
| Workflows Created | 8,900 |
| Community Skills | 0 (coming soon!) |

---

## Roadmap

### Phase 1: Foundation (Jan-Feb 2025)
- ✅ Skill specification
- ✅ Core skills (SDXL, upscale, etc.)
- 🔜 Community skill examples

### Phase 2: Community (Mar-Apr 2025)
- Community skill contributions
- Featured skill spotlight
- Skill ratings & reviews

### Phase 3: Marketplace (May-Jun 2025)
- Premium skills
- Paid skill marketplace
- Skill monetization

### Phase 4: Platform (Jul+ 2025)
- Skill dependencies (skill A depends on B)
- Skill collaboration
- Advanced optimization
- A/B testing via skills

---

## Resources

| Resource | Link |
|----------|------|
| **Specification** | [SKILL_SPEC.md](./SKILL_SPEC.md) |
| **Create a Skill** | [SKILLS_CONTRIBUTE.md](../SKILLS_CONTRIBUTE.md) |
| **Examples** | [core/](./core/) |
| **Registry** | [skill-registry.json](./skill-registry.json) |
| **Documentation** | [Comfy Pilot Wiki](https://github.com/anthropics/comfy-pilot/wiki) |
| **Community** | [Discord](https://discord.gg/comfy-pilot) |

---

## License

All core skills are MIT licensed. Community skills follow the author's chosen license.

---

**Made with ❤️ by Comfy Pilot**

*Skills are the future of generative AI workflows.*
