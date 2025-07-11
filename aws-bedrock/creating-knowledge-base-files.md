# Creating Your Own Knowledge Base Files for GenAI PowerUser

## Overview

This guide explains how to create your own custom knowledge base files for use with GenAI PowerUser. Custom knowledge files can enhance AI assistance by providing specialized context for your projects, teams, or domains.

## Where to Create Knowledge Files

You can create knowledge files in any directory of your choosing. Common locations include:

1. **Project-specific knowledge**: Within your project directory
   ```
   /path/to/your/project/.amazonq/knowledge/
   ```

2. **Personal knowledge base**: In a dedicated directory in your home folder
   ```
   ~/knowledge/
   ```

3. **Team knowledge base**: In a shared team repository
   ```
   /path/to/team-repo/knowledge/
   ```

## Step-by-Step Instructions

### 1. Create a directory for your knowledge files

```bash
# For a project-specific knowledge base
mkdir -p /path/to/your/project/.amazonq/knowledge/

# For a personal knowledge base
mkdir -p ~/knowledge/
```

### 2. Create a markdown file

Use any text editor or IDE to create a markdown (.md) file in your knowledge directory:

```bash
vim ~/knowledge/my-custom-knowledge.md
```

or

```bash
code ~/knowledge/my-custom-knowledge.md
```

### 3. Structure your knowledge file

A well-structured knowledge file typically includes:

```markdown
---
title: "My Custom Knowledge"
tags: [project, domain, category]
date: 2025-07-05
author: "Your Name"
---

# My Custom Knowledge Title

## Overview
A brief description of what this knowledge file contains.

## Key Concepts
- Concept 1: Description
- Concept 2: Description
- Concept 3: Description

## Examples

### Example 1
```python
# Sample code demonstrating a concept
def example_function():
    return "This is an example"
```

## References
- [Reference 1](URL)
- [Reference 2](URL)
```

### 4. Add your knowledge file to GenAI PowerUser context

Once you've created your knowledge file, you can add it to the GenAI PowerUser context using the following command in the Amazon Q CLI:

```
/context add ~/knowledge/my-custom-knowledge.md
```

You can also add entire directories of knowledge files:

```
/context add ~/knowledge/
```

For persistent context across sessions, add `--global` to the command:

```
/context add --global ~/knowledge/my-custom-knowledge.md
```

## Best Practices for Knowledge Files

### Organization

- **Use clear hierarchical structure**: Organize content with headings (# for main headings, ## for subheadings)
- **Group related knowledge**: Create separate files for different domains/topics
- **Use consistent naming**: Adopt a naming convention (e.g., domain-topic-subtopic.md)

### Content

- **Be concise and precise**: Focus on clear, factual information
- **Include examples**: Provide concrete examples where appropriate
- **Use code blocks**: Format code examples properly with language-specific formatting
- **Link related concepts**: Use markdown links to connect related knowledge

### Metadata

- **Use YAML frontmatter**: Include metadata at the top of your files
- **Add tags**: Tag your knowledge files for easier searching
- **Include dates and versions**: Note when knowledge was created or updated

## Using Your Knowledge

Once your knowledge files are added to the context, you can interact with them using GenAI PowerUser tools:

- **Read knowledge**: `genai_poweruser_read_knowledge`
- **Search knowledge**: `genai_poweruser_search_knowledge`
- **List knowledge**: `genai_poweruser_list_knowledge`
- **Get knowledge structure**: `genai_poweruser_get_knowledge_structure`
- **Get knowledge metadata**: `genai_poweruser_get_knowledge_metadata`

## Example Command Workflow

```bash
# Create a knowledge directory
mkdir -p ~/knowledge/project-x

# Create a knowledge file
touch ~/knowledge/project-x/architecture.md

# Edit the file with your preferred editor
code ~/knowledge/project-x/architecture.md

# Add the file to Amazon Q context
/context add ~/knowledge/project-x/architecture.md

# Search across your knowledge
/use genai_poweruser_search_knowledge query="architecture pattern"
```

## Conclusion

Creating your own knowledge base files allows you to customize and enhance the context available to AI assistants like Amazon Q. By following these guidelines, you can build a rich knowledge base that helps AI tools provide more relevant and accurate assistance for your specific domains and projects.
