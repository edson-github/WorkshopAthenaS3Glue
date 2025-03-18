-- Consulta exemplo: Vendas agrupadas por data
SELECT 
  DATE(data_venda) as data,
  COUNT(*) as num_vendas,
  SUM(quantidade) as total_itens,
  SUM(preco * quantidade) as valor_total
FROM "meu_database"."vendas"
GROUP BY DATE(data_venda)
ORDER BY data; 