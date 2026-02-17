# Contributing a Skill to Comfy Pilot

**Share your workflow with the world. Get featured. Build community reputation.**

---

## Quick Start

1. **Create** a working ComfyUI workflow
2. **Package** it as a Skill (follow template)
3. **Test** validation (`skill validate`)
4. **Submit** PR to repository
5. **Featured** in registry once approved

---

## Step-by-Step Guide

### Step 1: Build Your Workflow

Create and test a working ComfyUI workflow manually.

**Requirements:**
- ✅ Workflow should be useful/unique
- ✅ Tested and works reliably
- ✅ Clear inputs and outputs
- ✅ Well-documented parameters

### Step 2: Create Skill Structure

Copy this template:

```bash
mkdir -p skills/community/your-skill-name
cd skills/community/your-skill-name
```

Create these files:

```
your-skill-name/
├── skill.json           # Metadata (required)
├── workflow.json        # ComfyUI workflow (required)
├── README.md            # Documentation (required)
├── examples/            # Examples (recommended)
│   └── example1.md
└── docs/                # Additional docs (optional)
    └── optimization.md
```

### Step 3: Write `skill.json`

Use this template:

```json
{
  "id": "your-skill-id",
  "name": "Your Skill Name",
  "version": "1.0.0",
  "author": "your-github-username",
  "description": "One-line description of what it does",

  "category": "generation",
  "tags": ["tag1", "tag2", "tag3"],
  "repository": "https://github.com/YOUR_USERNAME/comfy-pilot/tree/main/skills/community/your-skill-name",

  "metadata": {
    "comfyui_min_version": "0.1.0",
    "python_min_version": "3.8",
    "dependencies": ["comfyui_core"],
    "stability": "beta"
  },

  "inputs": {
    "param_name": {
      "type": "string",
      "description": "What this parameter does",
      "default": "default_value",
      "required": true,
      "examples": ["example1", "example2"]
    }
  },

  "outputs": {
    "output_name": {
      "type": "image",
      "description": "What gets output"
    }
  },

  "nodes_created": ["NodeType1", "NodeType2"],

  "performance": {
    "estimated_time_seconds": 30,
    "estimated_vram_gb": 4,
    "estimated_ram_gb": 2,
    "device": "nvidia_gpu_recommended"
  },

  "examples": [
    {
      "name": "Example 1",
      "description": "What this example does"
    }
  ],

  "license": "MIT",
  "updated": "2025-01-28T17:00:00Z"
}
```

[Full `skill.json` Reference](./skills/SKILL_SPEC.md)

### Step 4: Export `workflow.json`

In ComfyUI:
1. Build your workflow
2. Click menu → "Save (API Format)"
3. Save as `workflow.json`
4. Place in your skill directory

### Step 5: Write `README.md`

Document for humans:

```markdown
# Your Skill Name

One-line description.

## What It Does

Explain what happens when someone uses this skill.

## Quick Start

Show the simplest usage example.

## Parameters

| Name | Type | Default | Description |
|------|------|---------|-------------|
| param1 | string | default | What it does |

## Examples

Provide 2-3 real usage examples.

## Performance

- Time: X seconds
- VRAM: X GB
- Best for: [use case]

## Troubleshooting

Common issues and fixes.

## License

MIT
```

### Step 6: Add Examples

Create `examples/` directory with real usage examples:

```markdown
# Portrait Photography Example

Use this skill to generate professional portraits.

## Settings

```json
{
  "param1": "value1",
  "param2": "value2"
}
```

## Result

[Description of typical output]
```

### Step 7: Validate

```bash
# In skills directory
python3 validate_skill.py skills/community/your-skill-name

# Should output:
# ✅ skill.json valid
# ✅ workflow.json valid
# ✅ README.md exists
# ✅ Examples documented
# Skill is ready for submission!
```

### Step 8: Submit PR

1. **Fork** the repository (if not contributor)
2. **Create branch**: `git checkout -b skills/your-skill-name`
3. **Add files** to `skills/community/your-skill-name/`
4. **Update** `skills/skill-registry.json` (add entry)
5. **Commit**: `git commit -m "Add: Your Skill Name skill"`
6. **Push** and **create PR**

### Step 9: Review & Featured

Community and maintainers will:
- ✅ Test your skill
- ✅ Review documentation
- ✅ Provide feedback
- ✅ Merge if approved
- ✅ Feature in registry

---

## Skill Quality Guidelines

### Naming Convention

✅ **Good:**
- `sdxl-generation`
- `upscale-4x`
- `controlnet-pose`
- `batch-processing`

❌ **Bad:**
- `my_cool_workflow`
- `test123`
- `skill`
- `workflow_final_v2_real`

### Documentation Standards

**Must have:**
- Clear one-line description
- Explanation of what it does
- All inputs/outputs documented
- At least one usage example
- Performance estimates

**Should have:**
- Multiple examples for different use cases
- Tips and best practices
- Troubleshooting section
- Link to source/inspiration

### Code Quality

- Skill JSON must be valid JSON
- Workflow must pass ComfyUI validation
- No hardcoded paths or credentials
- All parameters should have defaults
- Clear parameter descriptions

### Stability

- **Stable:** Production-ready, extensively tested
- **Beta:** Works but might change
- **Experimental:** Early stage, expect changes

Start as Beta. Move to Stable once tested.

---

## Common Mistakes

| Mistake | Problem | Fix |
|---------|---------|-----|
| No skill.json | Can't discover skill | Add valid skill.json |
| Missing examples | Unclear how to use | Add 2+ examples |
| Wrong JSON format | Validation fails | Validate before PR |
| Hardcoded values | Doesn't work for others | Use parameters |
| No documentation | Users confused | Write clear README |
| Requires external tools | Doesn't work standalone | Keep self-contained |

---

## Tips for Success

### 1. Solve Real Problems

Create skills for things people actually need.

❌ Generic and obvious
✅ Specific and useful

### 2. Document Thoroughly

Your README is marketing. Invest time here.

Good examples = more downloads.

### 3. Start with Beta

Mark as `beta` initially. Move to `stable` once tested by community.

### 4. Engage Community

- Announce your skill in Discord
- Ask for feedback
- Incorporate suggestions
- Version updates

### 5. Optimize Performance

Include realistic performance numbers:
- Test on multiple GPUs
- Document memory requirements
- Provide optimization tips

---

## Featured Skills

Top-tier skills get:

✅ Featured in registry README
✅ Highlighted in announcements
✅ Recommended when relevant
✅ Community spotlight

**Requirements for featured:**
- 100+ downloads
- 4.5+ rating
- Comprehensive documentation
- Active maintenance

---

## Monetization (Future)

We're planning a marketplace where creators can:
- Sell premium skills
- Earn revenue share
- Build reputation

Early contributors get priority in marketplace launch.

---

## Support

### Questions?

- **Discord:** [Ask in #skills channel](https://discord.gg/comfy-pilot)
- **Issues:** [GitHub Issues](https://github.com/anthropics/comfy-pilot/issues)
- **Template:** Copy from `skills/core/sdxl-generation/`

### Need Help?

1. Check [SKILL_SPEC.md](./skills/SKILL_SPEC.md)
2. Review existing skills in `skills/core/`
3. Ask in Discord community
4. Open GitHub discussion

---

## Review Process

When you submit a PR:

1. **Validation Check** (automated)
   - JSON valid?
   - Workflow valid?
   - README exists?
   - Examples included?

2. **Code Review** (maintainers)
   - Does it work?
   - Is documentation clear?
   - Does it solve a real problem?
   - Performance reasonable?

3. **Community Testing** (optional)
   - Featured creators test it
   - Feedback provided
   - Suggestions incorporated

4. **Merge**
   - PR approved
   - Added to registry
   - Announced to community

**Timeline:** 1-2 weeks from PR to featured.

---

## Examples

Check these for inspiration:

- **Core:** [SDXL Generation](./skills/core/sdxl-generation/)
- **Community:** (Coming soon!)

---

## License

Contributions must include a license (recommend MIT):

```
MIT License

Copyright (c) 2025 [Your Name]

Permission is hereby granted, free of charge...
```

---

## Code of Conduct

- ✅ Be respectful
- ✅ Give constructive feedback
- ✅ Help others
- ✅ Share knowledge

- ❌ No spam
- ❌ No misleading claims
- ❌ No plagiarism
- ❌ No malicious code

---

## Ready to Share?

1. Create your skill
2. Validate locally
3. Submit PR
4. Get featured
5. Build community
6. Earn reputation

**The community's best workflows power the platform. Let's build something great together.** 🚀

---

[Back to Skills README](./skills/README.md)
