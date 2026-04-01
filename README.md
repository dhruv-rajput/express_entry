# 🍁 Express Entry PR — Full Stack

**Python (FastAPI) + React** end-to-end application for Canada's Express Entry permanent residence process.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         React Frontend                       │
│   Dashboard │ Profile │ Documents │ Draws │ AI Chat │ Cases │
└──────────────────────────┬──────────────────────────────────┘
                           │ REST API + WebSocket
┌──────────────────────────▼──────────────────────────────────┐
│                     FastAPI Backend                          │
│  Auth │ Profile │ CRS Calculator │ Documents │ Draws │ AI   │
└────┬──────────────┬───────────────────────────┬─────────────┘
     │              │                           │
 PostgreSQL       Redis                    Azure Services
 (SQLAlchemy)  (Cache/Queue)           ┌───────────────────┐
                                       │ • OpenAI GPT-4o   │
              Celery Workers           │ • Doc Intelligence│
           ┌──────────────────┐        │ • Blob Storage    │
           │ • Draw Monitor   │        └───────────────────┘
           │ • AI Analysis    │
           │ • Reminders      │        ChromaDB (RAG)
           │ • Notifications  │
           └──────────────────┘
```

---

## Tech Stack

### Frontend (React)
| Package | Purpose |
|---|---|
| React 18 + Vite | UI framework + build |
| React Router v6 | Client-side routing |
| React Query | Server state, caching |
| Zustand | Global client state |
| Framer Motion | Animations |
| Recharts | Draw history charts |
| React Hook Form | Form management |
| React Dropzone | Document uploads |
| TailwindCSS | Styling |
| Lucide React | Icons |
| React Markdown | AI chat message rendering |

### Backend (Python)
| Package | Purpose |
|---|---|
| FastAPI | Web framework |
| SQLAlchemy (async) | ORM |
| PostgreSQL | Primary database |
| Redis | Cache + Celery broker |
| Celery + Celery Beat | Background jobs + scheduler |
| Azure OpenAI (GPT-4o) | LLM for AI features |
| Azure Document Intelligence | Document OCR |
| Azure Blob Storage | File storage |
| ChromaDB | Vector DB for RAG |
| Sentence Transformers | Text embeddings |
| python-jose | JWT authentication |
| SendGrid | Email notifications |
| Firebase | Push notifications |
| Twilio | SMS notifications |

---

## Project Structure

```
express_entry_fullstack/
│
├── frontend/                    ← React App
│   ├── src/
│   │   ├── App.jsx              ← Routing
│   │   ├── main.jsx
│   │   ├── styles/globals.css   ← Design system
│   │   ├── services/api.js      ← All API calls
│   │   ├── store/index.js       ← Zustand store
│   │   ├── hooks/index.js       ← React Query hooks + WebSocket + AI streaming
│   │   ├── components/
│   │   │   └── layout/Layout.jsx ← Sidebar + header + WS
│   │   └── pages/
│   │       ├── Auth.jsx          ← Login + Register
│   │       ├── Dashboard.jsx     ← CRS gauge, stats, charts, AI tips
│   │       ├── Profile.jsx       ← Multi-step profile builder
│   │       ├── Documents.jsx     ← Dropzone + AI review
│   │       ├── Draws.jsx         ← Draw tracker + charts + AI prediction
│   │       ├── Application.jsx   ← ITA checklist + deadline countdown
│   │       ├── Assistant.jsx     ← Streaming AI chat
│   │       └── NocFinder.jsx     ← AI NOC code matching
│   ├── Dockerfile
│   ├── nginx.conf
│   └── package.json
│
└── backend/                     ← FastAPI App
    ├── core/
    │   ├── domain/models.py      ← Domain entities
    │   └── application/services/
    │       └── crs_calculator.py ← Full CRS calculator
    ├── infrastructure/
    │   ├── ai/ai_services.py     ← GPT-4o, Doc Intel, RAG, NOC finder
    │   ├── persistence/database.py ← SQLAlchemy ORM
    │   ├── storage/blob_storage.py
    │   └── notifications/
    ├── api/main.py               ← All FastAPI routes
    ├── workers/tasks.py          ← Celery tasks
    ├── tests/unit/
    ├── requirements.txt
    └── Dockerfile
```

---

## Quick Start

### Option 1: Docker Compose (Recommended)

```bash
# Clone repo
git clone <repo>
cd express_entry_fullstack

# Configure backend
cp backend/.env.example backend/.env
# Edit backend/.env with your Azure credentials

# Start everything
docker-compose up -d

# App: http://localhost:3000
# API docs: http://localhost:8000/docs
# Flower (Celery): http://localhost:5555
# Adminer (DB): http://localhost:8080
```

### Option 2: Manual Development

**Backend:**
```bash
cd backend
pip install -r requirements.txt
cp .env.example .env

# Start DB + Redis
docker-compose up postgres redis chromadb -d

# API
uvicorn api.main:app --reload --port 8000

# Celery worker
celery -A workers.tasks.celery_app worker --loglevel=info

# Celery beat (scheduler)
celery -A workers.tasks.celery_app beat --loglevel=info
```

**Frontend:**
```bash
cd frontend
npm install
npm run dev
# → http://localhost:3000
```

---

## Pages

| Page | Path | Description |
|---|---|---|
| Login | `/login` | JWT authentication |
| Register | `/register` | Account creation |
| Dashboard | `/dashboard` | CRS gauge, draw chart, AI tips, stats |
| Profile | `/profile` | Multi-step: personal, language, work, education, other |
| Documents | `/documents` | Upload + AI OCR + GPT-4o review |
| Draw Tracker | `/draws` | All draws, bar chart, AI draw prediction |
| Application | `/application` | ITA checklist, deadline countdown, status tracker |
| NOC Finder | `/noc-finder` | AI-powered NOC code matching |
| AI Assistant | `/assistant` | Streaming RAG chatbot with profile context |

---

## AI Features

| Feature | Technology | Where |
|---|---|---|
| Document extraction | Azure Document Intelligence | Documents page |
| Document review | GPT-4o Vision | Documents page (AI Review modal) |
| NOC code matching | GPT-4o | NOC Finder page |
| CRS improvement tips | GPT-4o + draw data | Dashboard |
| Draw prediction | GPT-4o + historical draws | Dashboard + Draw Tracker |
| ITA checklist generation | GPT-4o | Application page (on ITA receipt) |
| Immigration chatbot | GPT-4o + RAG (IRCC docs) | AI Assistant page |
| Real-time draw alerts | WebSocket + Celery | System-wide notifications |

---

## API Endpoints

```
POST /api/v1/auth/register
POST /api/v1/auth/login

GET/POST  /api/v1/profile
POST      /api/v1/profile/language-tests
POST      /api/v1/profile/work-experience
POST      /api/v1/profile/education
GET       /api/v1/profile/ircc-ready    ← Browser extension endpoint

POST /api/v1/crs/calculate
GET  /api/v1/crs/history

POST /api/v1/documents/upload
GET  /api/v1/documents
GET  /api/v1/documents/{id}/review

GET /api/v1/draws
GET /api/v1/draws/stats

POST /api/v1/cases/ita-received
GET  /api/v1/cases/active
PATCH /api/v1/cases/checklist/{id}

POST /api/v1/ai/noc-finder
GET  /api/v1/ai/crs-improvements
GET  /api/v1/ai/draw-prediction
GET  /api/v1/ai/chat              ← SSE streaming

GET /api/v1/notifications
PATCH /api/v1/notifications/{id}/read

WS /ws/draws/{user_id}            ← Real-time draw alerts
```

---

## Browser Extension (Bonus)

Install `browser_extension/` in Chrome Developer mode to auto-fill your IRCC profile using data from your app. The user logs into IRCC themselves — the extension never handles credentials.

---

## Legal Notice

This app is not affiliated with IRCC or the Government of Canada. It provides tools and information to help applicants, not legal immigration advice. For complex situations, always consult a licensed RCIC (Regulated Canadian Immigration Consultant).
# Deployed via GitHub auto-deploy Tue Mar 31 06:56:50 IST 2026
# Railway deployment test

