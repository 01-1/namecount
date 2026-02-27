const form = document.getElementById('estimator-form');
const peopleList = document.getElementById('people-list');
const result = document.getElementById('result');
let currentSessionId = null;

function buildCheckbox(name, group) {
  const id = `${group}-${name.toLowerCase().replace(/\s+/g, '-')}`;
  const label = document.createElement('label');
  const input = document.createElement('input');
  input.type = 'checkbox';
  input.name = group;
  input.value = name;
  input.id = id;

  const text = document.createElement('span');
  text.textContent = name;

  label.appendChild(input);
  label.appendChild(text);
  return label;
}

async function loadLists() {
  const res = await fetch('/api/lists');
  const data = await res.json();
  currentSessionId = data.sessionId;
  peopleList.innerHTML = '';

  data.people.forEach((name) => {
    peopleList.appendChild(buildCheckbox(name, 'people'));
  });
}

function getChecked(name) {
  return Array.from(document.querySelectorAll(`input[name="${name}"]:checked`)).map((input) => input.value);
}

form.addEventListener('submit', async (event) => {
  event.preventDefault();

  const payload = {
    sessionId: currentSessionId,
    selectedPeople: getChecked('people')
  };

  const res = await fetch('/api/estimate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });

  const data = await res.json();

  if (!res.ok) {
    result.classList.remove('hidden');
    result.textContent = `Error: ${data.error || 'Unable to calculate estimate.'}`;
    return;
  }

  result.classList.remove('hidden');
  result.innerHTML = `
    <h2>Estimated People You Know: ${data.estimate.toLocaleString()}</h2>
    <p>Notable recognized: ${data.notableChecked}/${data.notableTotal}</p>
    <p>Fake names selected: ${data.fakeChecked}/${data.fakeTotal}</p>
    <p>Truthfulness score: ${data.honestyScore}/100</p>
    <p>Note that these estimates are inaccurate as the dataset has not been fully compiled!</p>
  `;
});

loadLists().catch((error) => {
  result.classList.remove('hidden');
  result.textContent = `Failed to load lists: ${error.message}`;
});
