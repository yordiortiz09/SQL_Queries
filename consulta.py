class Consulta:
    def __init__(self):
        self.query = """
        SELECT 
            o.OrderDate,
            c.CategoryID,
            c.CategoryName,
            p.ProductID,
            p.ProductName,
            od.UnitPrice,
            od.Quantity,
            o.CustomerID,
            YEAR(o.OrderDate) AS Year
        FROM `order details` od
        LEFT JOIN Orders o ON o.OrderID = od.OrderID
        INNER JOIN Products p ON p.ProductID = od.ProductID
        LEFT JOIN Categories c ON c.CategoryID = p.CategoryID
        """

    def get_query(self):
        return self.query
