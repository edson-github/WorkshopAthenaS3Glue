-- Consulta exemplo: Total de vendas por produto
SELECT 
  produto,
  SUM(quantidade) as total_itens,
  SUM(preco * quantidade) as valor_total
FROM "meu_database"."vendas"
GROUP BY produto
ORDER BY valor_total DESC; 