// ----------------------------
// 1️⃣ Criar constraints/índices
// ----------------------------
CREATE CONSTRAINT IF NOT EXISTS FOR (u:User) REQUIRE u.user_id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (p:Post) REQUIRE p.post_id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (c:Comment) REQUIRE c.comment_id IS UNIQUE;

// ----------------------------
// 2️⃣ Importar usuários
// ----------------------------
LOAD CSV WITH HEADERS FROM 'file:///users.csv' AS row
CREATE (:User {user_id: row.user_id});

// ----------------------------
// 3️⃣ Importar posts
// ----------------------------
LOAD CSV WITH HEADERS FROM 'file:///posts.csv' AS row
CREATE (:Post {
    post_id: row.id,
    title: row.title,
    selftext: row.selftext,
    permalink: row.permalink,
    created_utc: datetime({epochSeconds: toInteger(row.created_utc)}),
    score: toInteger(row.score)
});

// ----------------------------
// 4️⃣ Importar comentários
// ----------------------------
LOAD CSV WITH HEADERS FROM 'file:///comments.csv' AS row
CREATE (:Comment {
    comment_id: row.id,
    body: row.body,
    permalink: row.permalink,
    created_utc: datetime({epochSeconds: toInteger(row.created_utc)}),
    score: toInteger(row.score)
});

// ----------------------------
// 5️⃣ Criar relações: usuários -> posts
// ----------------------------
LOAD CSV WITH HEADERS FROM 'file:///posts.csv' AS row
MATCH (u:User {user_id: row.subreddit.id})
MATCH (p:Post {post_id: row.id})
CREATE (u)-[:POSTED]->(p);

// ----------------------------
// 6️⃣ Criar relações: usuários -> comentários
// ----------------------------
LOAD CSV WITH HEADERS FROM 'file:///comments.csv' AS row
MATCH (u:User {user_id: row.get('author_id', row.id)}) // fallback se author_id não existir
MATCH (c:Comment {comment_id: row.id})
CREATE (u)-[:COMMENTED]->(c);

// ----------------------------
// 7️⃣ Criar relações: comentários -> posts
// ----------------------------
LOAD CSV WITH HEADERS FROM 'file:///comments.csv' AS row
MATCH (c:Comment {comment_id: row.id})
MATCH (p:Post {post_id: row.permalink.split("/")[6]}) // extrai post_id do permalink
CREATE (c)-[:ON_POST]->(p);

// ----------------------------
// 8️⃣ Criar relações: comentários -> comentários (replies)
// ----------------------------
LOAD CSV WITH HEADERS FROM 'file:///comments.csv' AS row
WITH row WHERE row.parent_comment_id IS NOT NULL
MATCH (c:Comment {comment_id: row.id})
MATCH (parent:Comment {comment_id: row.parent_comment_id})
CREATE (c)-[:REPLY_TO]->(parent);