const copyButtonLabel = "Copy";

// ищем все pre-блоки
let blocks = document.querySelectorAll("pre");

blocks.forEach((block) => {
  // проверяем, есть ли внутри code и button
  let code = block.querySelector("code");
  let button = block.querySelector("button");

  if (!code || !button) return;

  button.addEventListener("click", async () => {
    await copyCode(code, button);
  });
});

async function copyCode(code, button) {
  const text = code.innerText;

  try {
    await navigator.clipboard.writeText(text);
    button.innerText = "Code Copied";

    setTimeout(() => {
      button.innerText = copyButtonLabel;
    }, 700);
  } catch (err) {
    console.error("Failed to copy:", err);
    button.innerText = "Error";
  }
}
